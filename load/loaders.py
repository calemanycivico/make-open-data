import os
import zipfile
import tempfile
from pathlib import Path
import csv
import pandas as pd
import geopandas as gpd
import snowflake.connector
import requests
from tqdm import tqdm

# ----------------------------
# Snowflake Connection Helpers
# ----------------------------
def get_sf_connection(sf_config):
    """
    Create and return a Snowflake connection using the configuration dictionary.
    """
    return snowflake.connector.connect(
        account=sf_config['account'],
        user=sf_config['user'],
        authenticator=sf_config['authenticator'],
        database=sf_config['database'],
        schema=sf_config['schema'].strip(),
        warehouse=sf_config['warehouse'],
        role=sf_config['role']
    )

def list_tables_in_sf(storage_to_sf, sf_config):
    """
    List the tables in the target Snowflake schema.
    """
    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()
    target_schema = list({data['db_schema'] for data in storage_to_sf.values()})[0]
    cs.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'".format(target_schema.upper())
    )
    result = cs.fetchall()
    cs.close()
    ctx.close()
    return [row[0] for row in result]

def get_table_row_count(table_name, sf_config, schema):
    """
    Returns the number of rows in the specified table.
    """
    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()
    query = f"SELECT COUNT(*) FROM {schema.upper()}.{table_name.upper()}"
    cs.execute(query)
    count = cs.fetchone()[0]
    cs.close()
    ctx.close()
    return count

# ----------------------------
# File Processing Helpers
# ----------------------------
def detect_csv_delimiter(file_path):
    """
    Detects whether a CSV file uses comma or semicolon as the delimiter.
    Reads the first nonblank line and counts commas vs semicolons.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                line = line.lstrip('\ufeff')
                comma_count = line.count(',')
                semicolon_count = line.count(';')
                return ';' if semicolon_count > comma_count else ','
    return ','

def load_shapefile_to_csv(zip_path, output_csv, simplify_tolerance=None):
    """
    Extracts a ZIP archive containing a shapefile, reprojects to EPSG:4326 if needed,
    optionally simplifies the geometry, converts it to WKT, and saves as CSV.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir)
        shp_files = list(Path(tmpdir).glob("*.shp"))
        if not shp_files:
            raise ValueError("No .shp file found inside the ZIP archive.")
        shp_file = str(shp_files[0])
        gdf = gpd.read_file(shp_file)
        if gdf.crs is not None and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs(epsg=4326)
        # Simplify geometry if tolerance is provided (default tolerance is passed in)
        if simplify_tolerance is not None:
            gdf["geometry"] = gdf["geometry"].simplify(tolerance=simplify_tolerance, preserve_topology=True)
        # Convert geometry to WKT for CSV export.
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)
        gdf.to_csv(output_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    return output_csv

def convert_to_parquet(input_csv, output_parquet):
    """
    Reads a CSV file and writes it as a Parquet file.
    """
    df = pd.read_csv(input_csv)
    df.to_parquet(output_parquet, index=False)
    return output_parquet

def load_file_from_storage(tmpfile_path, data_infos):
    """
    Download and process files from storage.
    Supports CSV, JSON, and SHP (zipped shapefiles).
    Optionally converts output to Parquet if specified.
    """
    url = data_infos["storage_path"]
    file_format = data_infos["file_format"]
    output_format = data_infos.get("output_format", "csv").lower()

    # Download the original file
    if file_format in ["csv", "json"]:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 1024
        with open(tmpfile_path, "wb") as f, tqdm(total=total_size, unit="B", unit_scale=True, 
                                                   desc=f"Downloading {Path(tmpfile_path).name}", leave=False) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
        # If JSON, convert to CSV
        if file_format == "json":
            df = pd.read_json(tmpfile_path)
            df.to_csv(tmpfile_path, index=False)
    elif file_format in ["shp", "shape"]:
        zip_path = tmpfile_path + ".zip"
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 1024
        with open(zip_path, "wb") as f, tqdm(total=total_size, unit="B", unit_scale=True, 
                                               desc=f"Downloading {Path(zip_path).name}", leave=False) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
        # Use default simplify tolerance of 0.01 (or one provided in the config)
        tol = data_infos.get("simplify_tolerance", 0.01)
        load_shapefile_to_csv(zip_path, tmpfile_path, simplify_tolerance=tol)
        os.remove(zip_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Optionally convert the output to Parquet
    if output_format == "parquet":
        parquet_file = tmpfile_path.rsplit(".", 1)[0] + ".parquet"
        convert_to_parquet(tmpfile_path, parquet_file)
        os.remove(tmpfile_path)
        return parquet_file  # Return the parquet file path
    else:
        return tmpfile_path

# ----------------------------
# Stage and Copy Functions
# ----------------------------
def stage_file_to_sf(local_file_path, table_name, data_infos, sf_config):
    """
    Creates the table if needed and uploads the file to the internal stage.
    The file is NOT removed after staging.
    Returns the staged file name.
    """
    # Determine delimiter if CSV and DVF (for logging purposes)
    configured_delim = data_infos.get("csv_delimiter", ",")
    delimiter = configured_delim
    if data_infos["file_format"] == "csv" and table_name.lower().startswith("dvf_"):
        detected = detect_csv_delimiter(local_file_path)
        if detected != configured_delim:
            print(f"Warning: Detected delimiter '{detected}' for {table_name} differs from configured '{configured_delim}'. Using detected.")
        delimiter = detected

    target_schema = data_infos["db_schema"].upper()
    target_table = table_name.upper()

    # Create table based on the header of the file (using CSV reading; Parquet reading is more involved)
    if data_infos.get("output_format", "csv").lower() == "csv":
        df = pd.read_csv(local_file_path, nrows=1, sep=delimiter)
    else:
        # For Parquet, read the file with pandas
        df = pd.read_parquet(local_file_path, engine="pyarrow")
    columns = df.columns.tolist()

    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()

    column_defs = []
    for col in columns:
        if col.lower() == "geometry" and data_infos.get("file_format") in ["shp", "shape"]:
            column_defs.append(f'"{col}" GEOGRAPHY')
        else:
            column_defs.append(f'"{col}" VARCHAR')
    create_table_stmt = (
        f"CREATE OR REPLACE TABLE {target_schema}.{target_table} ("
        + ", ".join(column_defs)
        + ")"
    )
    cs.execute(create_table_stmt)

    # Create (or re-create) the stage
    stage_name = f"{sf_config['database']}.{target_schema}.STG_DATA"
    cs.execute(f"CREATE OR REPLACE STAGE {stage_name};")

    file_uri = Path(local_file_path).as_uri()
    # For now, we disable auto compression so we can track the files as they are.
    put_stmt = f"PUT '{file_uri}' @{stage_name} AUTO_COMPRESS=FALSE;"
    cs.execute(put_stmt)

    ctx.commit()
    cs.close()
    ctx.close()

    staged_file_name = os.path.basename(local_file_path)
    print(f"Staged file for table {target_table}: {staged_file_name}")
    return staged_file_name

def copy_file_from_stage(staged_file, table_name, data_infos, sf_config):
    """
    Copies a single file from the internal stage into the target table.
    Does not remove the staged file.
    """
    target_schema = data_infos["db_schema"].upper()
    target_table = table_name.upper()
    output_format = data_infos.get("output_format", "csv").lower()

    # Choose file format settings based on output format.
    if output_format == "csv":
        configured_delim = data_infos.get("csv_delimiter", ",")
        delimiter = configured_delim  # (you could add more detection logic if needed)
        file_format_clause = f"""
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '{delimiter}'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
        )
        """
    elif output_format == "parquet":
        file_format_clause = "FILE_FORMAT = (TYPE = 'PARQUET')"
    else:
        raise ValueError(f"Unsupported output_format: {output_format}")

    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()
    stage_name = f"{sf_config['database']}.{target_schema}.STG_DATA"

    copy_stmt = f"""
    COPY INTO {target_schema}.{target_table}
    FROM @{stage_name}/{staged_file}
    {file_format_clause};
    """
    print(f"Copying from stage for table {target_table} using file {staged_file}...")
    cs.execute(copy_stmt)
    ctx.commit()
    cs.close()
    ctx.close()

# ----------------------------
# Overall Processing Logic
# ----------------------------
# Global dictionary to track staged files.
# Keys are table names; values are dicts with 'staged_file' and 'data_infos'
staged_files_mapping = {}

def process_file(tmpfile_path, data_infos, table_name, sf_config, storage_to_sf):
    """
    Checks if the table exists and downloads/stages the file into Snowflake.
    Does not run the COPY command immediately.
    """
    existing_tables = list_tables_in_sf(storage_to_sf, sf_config)
    target_table = table_name.upper()
    target_schema = data_infos["db_schema"]
    if target_table in (tbl.upper() for tbl in existing_tables):
        count = get_table_row_count(target_table, sf_config, target_schema)
        if count > 0:
            print(f"Table '{target_table}' already exists with {count} rows. Skipping download and staging.")
            return
        else:
            print(f"Table '{target_table}' exists but has 0 rows. Proceeding with staging.")
    # Download (and optionally convert) the file.
    local_file = load_file_from_storage(tmpfile_path, data_infos)
    # Stage the file into Snowflake (do not remove it later).
    staged_file = stage_file_to_sf(local_file, table_name, data_infos, sf_config)
    # Save the mapping for later copying.
    staged_files_mapping[table_name] = {
        "staged_file": staged_file,
        "data_infos": data_infos
    }
    print(f"Staged {table_name} into Snowflake.")

def process_source(table_name, data_infos, sf_config, storage_to_sf):
    print(f"Processing {table_name} ...")
    # Create a temporary file name in the system temp directory using the table name.
    tmp_dir = tempfile.gettempdir()
    # Choose the file extension based on the output format.
    file_ext = '.csv'
    if data_infos.get("output_format", "csv").lower() == "parquet":
        file_ext = '.parquet'
    tmp_path = os.path.join(tmp_dir, f"{table_name}{file_ext}")
    
    try:
        process_file(tmp_path, data_infos, table_name, sf_config, storage_to_sf)
    finally:
        # Remove or comment out the deletion if you want to keep the files.
        # if os.path.exists(tmp_path):
        #     os.remove(tmp_path)
        pass
    print(f"Finished processing {table_name}\n***")
    
# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    import yaml
    import concurrent.futures
    from pathlib import Path

    # Get the base directory (same folder as this script)
    BASE_DIR = Path(__file__).resolve().parent

    # Load Snowflake connection details from profiles.yml
    with open(BASE_DIR / "profiles.yml", "r") as pf:
        profiles = yaml.safe_load(pf)
    sf_config = profiles['makeopendata']['outputs'][profiles['makeopendata']['target']]

    # Load storage configuration from storage_to_pg.yml
    with open(BASE_DIR / "storage_to_pg.yml", "r") as sf:
        storage_to_sf = yaml.safe_load(sf)

    # First Phase: Stage all files
    existing_tables = list_tables_in_sf(storage_to_sf, sf_config)
    sources = list(storage_to_sf.items())
    total_files = len(sources)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor, tqdm(total=total_files, desc="Staging Files") as pbar:
        futures = {
            executor.submit(process_source, table_name, data_infos, sf_config, storage_to_sf): table_name
            for table_name, data_infos in sources
        }
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{table} generated an exception during staging: {exc}")
            pbar.update(1)

    print("\nAll files have been staged. Staged files mapping:")
    for tbl, info in staged_files_mapping.items():
        print(f"  {tbl}: {info['staged_file']}")

    # Second Phase: Copy files from the stage into the tables one by one.
    print("\nStarting copy phase from staged files...")
    for table, mapping in staged_files_mapping.items():
        try:
            copy_file_from_stage(mapping["staged_file"], table, mapping["data_infos"], sf_config)
            print(f"Copied data into table {table}.")
        except Exception as exc:
            print(f"Error copying staged file for {table}: {exc}")

    print("\nProcessing complete. All staged files remain in the internal stage for your review.")
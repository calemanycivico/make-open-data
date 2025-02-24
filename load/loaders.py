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
    # Assumes all entries in storage_to_sf share the same target schema.
    target_schema = list({data['db_schema'] for data in storage_to_sf.values()})[0]
    cs.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'".format(target_schema.upper())
    )
    result = cs.fetchall()
    cs.close()
    ctx.close()
    return [row[0] for row in result]

def load_shapefile_to_csv(zip_path, output_csv):
    """
    Extracts a ZIP archive containing a shapefile, converts the geometry to WKT in EPSG:4326,
    and saves it as a CSV file for Snowflake ingestion.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Extract all files inside the temporary directory
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir)

        # Locate the .shp file
        shp_files = list(Path(tmpdir).glob("*.shp"))
        if not shp_files:
            raise ValueError("No .shp file found inside the ZIP archive.")

        shp_file = str(shp_files[0])

        # Read the shapefile with GeoPandas
        gdf = gpd.read_file(shp_file)

        # If the CRS is not EPSG:4326, reproject it.
        if gdf.crs is not None and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs(epsg=4326)

        # Convert geometry to WKT for Snowflake ingestion
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)

        # Save as CSV
        gdf.to_csv(output_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    
    return output_csv

def load_file_from_storage(tmpfile_csv_path, data_infos):
    """
    Download and process files from storage. 
    Supports CSV, JSON, and SHP (zipped shapefiles).
    """
    url = data_infos["storage_path"]
    file_format = data_infos["file_format"]

    if file_format in ["csv", "json"]:
        # Stream the download
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 1024

        with open(tmpfile_csv_path, "wb") as f, tqdm(
            total=total_size, unit="B", unit_scale=True, 
            desc=f"Downloading {Path(tmpfile_csv_path).name}", leave=False
        ) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

        # Convert JSON -> CSV if needed
        if file_format == "json":
            df = pd.read_json(tmpfile_csv_path)
            df.to_csv(tmpfile_csv_path, index=False)

        return tmpfile_csv_path

    elif file_format == "shp":
        # We assume the shapefile is zipped
        zip_path = tmpfile_csv_path + ".zip"

        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 1024

        with open(zip_path, "wb") as f, tqdm(
            total=total_size, unit="B", unit_scale=True, 
            desc=f"Downloading {Path(zip_path).name}", leave=False
        ) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

        # Convert the shapefile to CSV with WKT geometry
        csv_path = load_shapefile_to_csv(zip_path, tmpfile_csv_path)
        os.remove(zip_path)
        return csv_path

    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def load_file_to_sf(tmpfile_csv_path, table_name, data_infos, sf_config):
    """
    Load a CSV file into Snowflake, including spatial data if applicable.
    In this "preprocess" approach, we create the table with the correct column types.
    For shapefiles, if a "geometry" column exists, it is defined as GEOGRAPHY.
    """
    delimiter = data_infos.get("csv_delimiter", ",")
    target_schema = data_infos["db_schema"].upper()
    target_table = table_name.upper()

    # Determine column names from the CSV using the correct delimiter.
    df = pd.read_csv(tmpfile_csv_path, nrows=1, sep=delimiter)
    columns = df.columns.tolist()

    # Connect to Snowflake.
    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()

    # Build table definition.
    # For shapefiles (file_format "shp"), create "geometry" as GEOGRAPHY.
    column_defs = []
    for col in columns:
        if col.lower() == "geometry" and data_infos.get("file_format") == "shp":
            column_defs.append(f'"{col}" GEOGRAPHY')
        else:
            column_defs.append(f'"{col}" VARCHAR')
    create_table_stmt = (
        f"CREATE OR REPLACE TABLE {target_schema}.{target_table} ("
        + ", ".join(column_defs)
        + ")"
    )
    cs.execute(create_table_stmt)

    # Create or replace stage.
    stage_name = f"{sf_config['database']}.{target_schema}.STG_DATA"
    cs.execute(f"CREATE OR REPLACE STAGE {stage_name};")

    # Upload file to Snowflake stage.
    file_uri = Path(tmpfile_csv_path).as_uri()
    put_stmt = f"PUT '{file_uri}' @{stage_name} AUTO_COMPRESS=FALSE;"
    cs.execute(put_stmt)

    # Copy data into Snowflake.
    # The FIELD_OPTIONALLY_ENCLOSED_BY option tells Snowflake that fields might be quoted,
    # so any commas within quotes (e.g. in WKT) will not be treated as delimiters.
    copy_stmt = f"""
    COPY INTO {target_schema}.{target_table}
    FROM @{stage_name}/{os.path.basename(tmpfile_csv_path)}
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = '{delimiter}'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    );
    """
    cs.execute(copy_stmt)

    # Cleanup stage.
    cs.execute(f"REMOVE @{stage_name};")
    ctx.commit()
    cs.close()
    ctx.close()

def process_file(tmpfile_csv_path, data_infos, table_name, sf_config, storage_to_sf):
    """
    Checks if the table exists and, if not, downloads the file and loads it into Snowflake.
    """
    existing_tables = list_tables_in_sf(storage_to_sf, sf_config)
    if table_name.upper() in (tbl.upper() for tbl in existing_tables):
        print(f"Table '{table_name.upper()}' already exists. Skipping download and load.")
        return
    
    downloaded_file = load_file_from_storage(tmpfile_csv_path, data_infos)
    load_file_to_sf(downloaded_file, table_name, data_infos, sf_config)
    print(f"Loaded {table_name} into Snowflake.") 
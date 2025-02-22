import os
import subprocess
from tempfile import mkstemp, NamedTemporaryFile
import shutil
from pathlib import Path

import pandas as pd
import snowflake.connector

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

def load_file_from_storage(tmpfile_csv_path, data_infos):
    """
    Download a CSV (or JSON file converted to CSV) from a remote URL.
    Uses curl to download the file.
    """
    storage_path = data_infos["storage_path"]
    
    if data_infos['file_format'] == 'csv':
        subprocess.run(['curl', '-o', tmpfile_csv_path, storage_path], check=True)
    
    elif data_infos['file_format'] == 'json':
        # Use mkstemp for JSON to avoid file locking issues on Windows.
        fd_json, tmp_json_path = mkstemp(suffix='.json')
        os.close(fd_json)
        subprocess.run(['curl', '-o', tmp_json_path, storage_path], check=True)
        data = pd.read_json(tmp_json_path)
        data.to_csv(tmpfile_csv_path, index=False)
        os.remove(tmp_json_path)
    else:
        raise ValueError(f"Unsupported file format in path: {storage_path}")
    
    return tmpfile_csv_path

def get_columns_from_csv(file_path, delimiter):
    """
    Dynamically read the first line of the CSV to get the correct column names.
    This approach strips any extraneous quotes that might be present in the header.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        header_line = f.readline().strip()
    # Split on the delimiter, strip whitespace and any surrounding double quotes.
    columns = [col.strip().strip('"') for col in header_line.split(delimiter) if col.strip() != '']
    return columns

def load_file_to_sf(tmpfile_csv_path: str, table_name: str, data_infos, sf_config):
    """
    Load a CSV file into Snowflake by:
      - Creating/replacing the target table in the schema specified by data_infos["db_schema"]
      - Creating (if needed) an internal stage at <database>.<db_schema>.STG_DATA
      - Creating a temporary file format (as defined in YAML)
      - Using the PUT command to upload the local file into the stage
      - Using COPY INTO to load the data into the table
      - Dropping the temporary file format and cleaning up the stage
    """
    delimiter = data_infos.get("csv_delimiter", ",")
    columns = get_columns_from_csv(tmpfile_csv_path, delimiter)
    target_schema = data_infos["db_schema"].upper()  # Use uppercase for Snowflake
    target_table = table_name.upper()                # Unquoted, so Snowflake stores in uppercase

    # Connect to Snowflake
    ctx = get_sf_connection(sf_config)
    cs = ctx.cursor()
    
    # Create (or replace) the target table with the dynamically derived columns.
    create_table_stmt = (
        f"CREATE OR REPLACE TABLE {target_schema}.{target_table} ("
        + ", ".join([f'"{col}" VARCHAR' for col in columns])
        + ")"
    )
    cs.execute(create_table_stmt)
    
    # Define the stage name based on database and target_schema.
    stage_name = f"{sf_config['database']}.{target_schema}.STG_DATA"
    
    # Create (or replace) the internal stage.
    cs.execute(f"CREATE OR REPLACE STAGE {stage_name};")
    
    # Create a temporary file format for this file.
    file_format_name = f"FF_{target_table}"
    cs.execute(
        f"CREATE OR REPLACE FILE FORMAT {file_format_name} "
        f"TYPE = 'CSV' FIELD_DELIMITER = '{delimiter}' SKIP_HEADER = 1 "
        f"ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;"
    )
    
    # Convert the local file path to a proper file URI.
    file_uri = Path(tmpfile_csv_path).as_uri()
    
    # Upload the file to the internal stage.
    put_stmt = f"PUT '{file_uri}' @{stage_name} AUTO_COMPRESS=FALSE;"
    cs.execute(put_stmt)
    
    # Load data from the staged file into the target table.
    copy_stmt = (
        f"COPY INTO {target_schema}.{target_table} "
        f"FROM @{stage_name}/{os.path.basename(tmpfile_csv_path)} "
        f"FILE_FORMAT = (FORMAT_NAME = '{file_format_name}');"
    )
    cs.execute(copy_stmt)
    
    # Drop the temporary file format.
    cs.execute(f"DROP FILE FORMAT IF EXISTS {file_format_name};")
    
    # Optionally, remove the file from the stage.
    cs.execute(f"REMOVE @{stage_name};")
    
    ctx.commit()
    cs.close()
    ctx.close()

import yaml
import os
import sys
from pathlib import Path
from tempfile import mkstemp, TemporaryDirectory

# Adjust the following paths as needed.
BASE_DIR = Path(__file__).resolve().parent  # directory of __main__.py

# Load Snowflake profile from profiles.yml (assuming it's in the same directory as __main__.py)
with open(BASE_DIR / "profiles.yml", "r") as pf:
    profiles = yaml.safe_load(pf)

sf_config = profiles['makeopendata']['outputs'][profiles['makeopendata']['target']]

# Load your storage configuration (adjust the path if needed)
with open(BASE_DIR / "storage_to_pg.yml", "r") as sf:
    storage_to_sf = yaml.safe_load(sf)

from load.loaders import (
    load_file_from_storage,
    load_file_to_sf,
    list_tables_in_sf
)

tables_in_sf = list_tables_in_sf(storage_to_sf, sf_config)

for table_name, data_infos in storage_to_sf.items():
    if table_name in tables_in_sf:
        print(f"Table already exists: {table_name}")
    else:
        print(f"Processing {table_name}")
        # Create a temporary CSV file using mkstemp (so it's not locked on Windows)
        fd, tmp_csv_path = mkstemp(suffix='.csv')
        os.close(fd)
        print(f"Downloading file for {table_name} ...")
        load_file_from_storage(tmp_csv_path, data_infos)
        print(f"Loading {table_name} into Snowflake ...")
        load_file_to_sf(tmp_csv_path, table_name, data_infos, sf_config)
        os.remove(tmp_csv_path)
        print("***")

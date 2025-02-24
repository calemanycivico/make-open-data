import yaml
import os
import concurrent.futures
from pathlib import Path
from tempfile import mkstemp

from tqdm import tqdm

# Get the base directory (same folder as __main__.py)
BASE_DIR = Path(__file__).resolve().parent

# Load Snowflake connection details from profiles.yml
with open(BASE_DIR / "profiles.yml", "r") as pf:
    profiles = yaml.safe_load(pf)
sf_config = profiles['makeopendata']['outputs'][profiles['makeopendata']['target']]

# Load storage configuration from storage_to_pg.yml
with open(BASE_DIR / "storage_to_pg.yml", "r") as sf:
    storage_to_sf = yaml.safe_load(sf)

# Import functions from your loaders module.
from load.loaders import process_file, list_tables_in_sf

# Get a list of tables already present in the target Snowflake schema.
existing_tables = list_tables_in_sf(storage_to_sf, sf_config)

def process_source(table_name, data_infos):
    """
    Process a single source:
      - Check if the table exists.
      - If not, download and load the file into Snowflake.
    """
    # Skip if table exists (case-insensitive)
    if table_name.upper() in (tbl.upper() for tbl in existing_tables):
        print(f"Table already exists: {table_name}. Skipping download and load.")
        return

    print(f"Processing {table_name} ...")
    # Create a temporary CSV file path.
    fd, tmp_csv_path = mkstemp(suffix='.csv')
    os.close(fd)
    
    try:
        # Use the loader logic to download and load into Snowflake
        process_file(tmp_csv_path, data_infos, table_name, sf_config, storage_to_sf)
    finally:
        # Clean up temporary file
        try:
            os.remove(tmp_csv_path)
        except Exception as e:
            print(f"Error cleaning up temporary file for {table_name}: {e}")
    
    print(f"Finished processing {table_name}\n***")

if __name__ == "__main__":
    # Use ThreadPoolExecutor with a progress bar for overall progress.
    sources = list(storage_to_sf.items())
    total_files = len(sources)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor, tqdm(total=total_files, desc="Overall Progress") as pbar:
        futures = {
            executor.submit(process_source, table_name, data_infos): table_name
            for table_name, data_infos in sources
        }
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{table} generated an exception: {exc}")
            pbar.update(1)

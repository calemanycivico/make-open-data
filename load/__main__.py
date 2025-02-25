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

# Import functions and global variables from your loaders module.
from load.loaders import process_source, list_tables_in_sf, staged_files_mapping, copy_file_from_stage

# Get a list of tables already present in the target Snowflake schema.
existing_tables = list_tables_in_sf(storage_to_sf, sf_config)

def main():
    # First Phase: Stage files that need loading.
    sources = list(storage_to_sf.items())
    total_files = len(sources)
    
    print("Starting staging phase...")
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
    
    print("\nStaging phase complete. Staged files mapping:")
    for tbl, mapping in staged_files_mapping.items():
        print(f"  {tbl}: {mapping['staged_file']}")
    
    # Second Phase: Copy files from the stage into their respective tables.
    print("\nStarting copy phase...")
    for table, mapping in staged_files_mapping.items():
        try:
            copy_file_from_stage(mapping["staged_file"], table, mapping["data_infos"], sf_config)
            print(f"Copied data into table {table}.")
        except Exception as exc:
            print(f"Error copying staged file for {table}: {exc}")
    
    print("\nProcessing complete. All staged files remain in the internal stage for your review.")

if __name__ == "__main__":
    main()

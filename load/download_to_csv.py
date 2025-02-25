#!/usr/bin/env python3
import os
import argparse
import requests
import zipfile
import tempfile
import csv
import pandas as pd
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm

def detect_csv_delimiter(file_path):
    """
    Detect if a CSV file uses a comma or semicolon delimiter.
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
        if simplify_tolerance is not None:
            gdf["geometry"] = gdf["geometry"].simplify(tolerance=simplify_tolerance, preserve_topology=True)
        # Convert geometry to WKT for CSV export.
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)
        gdf.to_csv(output_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    return output_csv

def download_file(url, output_path):
    """
    Downloads a file from the given URL to the specified output path using streaming.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))
    chunk_size = 1024
    with open(output_path, "wb") as f, tqdm(total=total_size, unit="B", unit_scale=True, desc=Path(output_path).name) as pbar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))
    return output_path

def process_file(url, file_format, output_csv, simplify_tolerance=None):
    """
    Downloads the file from the URL and exports it to CSV.
    """
    if file_format in ['csv', 'json']:
        # Download directly into output_csv
        download_file(url, output_csv)
        # If it's a JSON file, convert to CSV.
        if file_format == 'json':
            df = pd.read_json(output_csv)
            df.to_csv(output_csv, index=False)
        return output_csv
    elif file_format == 'shp':
        # For shapefiles, download the zip archive, then extract and convert.
        zip_path = output_csv + ".zip"
        download_file(url, zip_path)
        load_shapefile_to_csv(zip_path, output_csv, simplify_tolerance=simplify_tolerance)
        os.remove(zip_path)
        return output_csv
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def main():
    parser = argparse.ArgumentParser(description="Download a file and export it to CSV for manual inspection.")
    parser.add_argument("url", help="URL of the file to download")
    parser.add_argument("file_format", choices=["csv", "json", "shp"], help="Format of the file at the URL")
    parser.add_argument("--output", default="output.csv", help="Path to save the output CSV file")
    parser.add_argument("--simplify_tolerance", type=float, default=None, help="Simplify tolerance for shapefile conversion (if applicable)")
    args = parser.parse_args()

    print(f"Downloading file from {args.url} as format {args.file_format} ...")
    process_file(args.url, args.file_format, args.output, args.simplify_tolerance)
    print(f"File saved to {args.output}")

if __name__ == "__main__":
    main()

"""
Fuzzy Dataset Generator for Places, Neighborhoods, and Streets
==============================================================

This module extracts unique names from processed Google Places CSV, SIGA neighborhoods CSV,
and OSM street layers to create CSV datasets suitable for fuzzy matching.

It generates three CSV files in the FUZZY_DATA_DIR:
- places_names.csv
- neighborhood_names.csv
- street_names.csv

Dependencies
------------
- os (standard library)
- pandas
- geopandas
- config.paths (custom module providing directory paths)

Usage
-----
Run the script directly to generate the fuzzy datasets:

    $ python generate_fuzzy_datasets.py

Make sure the paths to the processed Google Places, SIGA, and OSM files are correctly set
at the bottom of the script in PLACES_FILE, NEIGHBORHOODS_FILE, and OSM_FILE.
"""

import os
import pandas as pd
import geopandas as gpd
from config.paths import PROCESSED_GOOGLE_PLACES_DIR, PROCESSED_SANTO_ANDRE_SIGA_DIR, RAW_SANTO_ANDRE_OSM_DIR, FUZZY_DATA_DIR

def extract_place_names(places_file: str) -> pd.DataFrame:
    """
    Extract unique place names from the Google Places CSV.

    Parameters
    ----------
    places_file : str
        Path to the CSV file containing processed Google Places data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a single column 'name' containing unique place names.
    """
    df = pd.read_csv(places_file, sep=";")
    place_names = df[['place_name']].drop_duplicates().dropna()
    return place_names.rename(columns={'place_name': 'name'})

def extract_neighborhood_names(neighborhoods_file: str) -> pd.DataFrame:
    """
    Extract unique neighborhood names from the processed SIGA CSV.

    Parameters
    ----------
    neighborhoods_file : str
        Path to the CSV file containing processed neighborhoods data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a single column 'name' containing unique neighborhood names.
    """
    df = pd.read_csv(neighborhoods_file, sep=";")
    neighborhood_names = df[['name']].drop_duplicates().dropna()
    return neighborhood_names

def extract_street_names(osm_file: str, layer_name: str = "edges") -> pd.DataFrame:
    """
    Extract unique street names from the OSM GeoPackage.

    Parameters
    ----------
    osm_file : str
        Path to the GeoPackage containing OSM layers.
    layer_name : str, optional
        Name of the layer containing street data, by default "edges".

    Returns
    -------
    pd.DataFrame
        DataFrame with a single column 'name' containing unique street names.
    """
    gdf = gpd.read_file(osm_file, layer=layer_name)
    street_names = gdf[['name']].drop_duplicates().dropna()
    return street_names

def export_to_csv(df: pd.DataFrame, filename: str) -> None:
    """
    Export a DataFrame to CSV in the FUZZY_DATA_DIR folder.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to export.
    filename : str
        Name of the CSV file to create.

    Returns
    -------
    None
    """
    output_path = os.path.join(FUZZY_DATA_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"Exported: {filename}")

def generate_fuzzy_datasets(places_file: str, neighborhoods_file: str, osm_file: str) -> None:
    """
    Generate CSV datasets for fuzzy matching: places, neighborhoods, and streets.

    Parameters
    ----------
    places_file : str
        Path to the Google Places CSV.
    neighborhoods_file : str
        Path to the neighborhoods CSV.
    osm_file : str
        Path to the OSM GeoPackage.

    Returns
    -------
    None
    """
    places_df = extract_place_names(places_file)
    neighborhoods_df = extract_neighborhood_names(neighborhoods_file)
    streets_df = extract_street_names(osm_file)

    export_to_csv(places_df, "places_names.csv")
    export_to_csv(neighborhoods_df, "neighborhood_names.csv")
    export_to_csv(streets_df, "street_names.csv")

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    PLACES_FILE = os.path.join(PROCESSED_GOOGLE_PLACES_DIR, "reviews_processed.csv")
    NEIGHBORHOODS_FILE = os.path.join(PROCESSED_SANTO_ANDRE_SIGA_DIR, "neighborhoods_processed.csv")
    OSM_FILE = os.path.join(RAW_SANTO_ANDRE_OSM_DIR, "santo_andre_osm_layers.gpkg")

    generate_fuzzy_datasets(PLACES_FILE, NEIGHBORHOODS_FILE, OSM_FILE)
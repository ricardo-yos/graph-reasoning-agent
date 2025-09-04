"""
Places JSON to GeoJSON Conversion
=================================

This script converts a JSON file containing place information (including coordinates, reviews, and other attributes)
into a GeoJSON file with Point geometries for geospatial analysis.

Main steps:
1. Load the JSON data and normalize it into a tabular format.
2. Optionally filter places by city.
3. Create a geometry column from latitude and longitude.
4. Convert the DataFrame to a GeoDataFrame.
5. Export the GeoDataFrame to a GeoJSON file.

Modules used:
- pandas: for data manipulation
- geopandas: for geospatial operations
- shapely: to construct Point geometries
- json: for reading JSON files
- os: for file path handling
"""

import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from config.paths import RAW_GOOGLE_PLACES_DIR, PROCESSED_GOOGLE_PLACES_DIR

# ==============================
# FUNCTION TO CONVERT JSON TO GEOJSON
# ==============================

def convert_places_json_to_geojson(json_path: str, geojson_path: str, city_name: str = None) -> None:
    """
    Convert a JSON file containing places with coordinates into a GeoJSON file with Point geometries.

    Parameters
    ----------
    json_path : str
        Path to the input JSON file. Must contain 'latitude' and 'longitude' fields.
    geojson_path : str
        Path where the output GeoJSON file will be saved.
    city_name : str, optional
        If provided, filter the places to only include this city.

    Raises
    ------
    ValueError
        If 'latitude' or 'longitude' are missing in any place entry.

    Notes
    -----
    Assumes the coordinate reference system is WGS84 (EPSG:4326).
    """

    # Load the JSON data and normalize into a DataFrame
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # Normalize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(' ', '_')
        .str.replace(r'[^a-z0-9_]', '', regex=True)
    )

    # Optional filtering by city
    if city_name and "city" in df.columns:
        df = df[df["city"].str.lower() == city_name.lower()]

    # Ensure latitude and longitude are present
    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError("Missing 'latitude' or 'longitude' fields in the JSON file.")

    # Create geometry column and convert to GeoDataFrame
    df["geometry"] = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Export to GeoJSON
    gdf.to_file(geojson_path, driver="GeoJSON")

    print(f"GeoJSON saved at: {geojson_path}")
    print(f"Number of places saved: {len(gdf)}")

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    json_input = os.path.join(RAW_GOOGLE_PLACES_DIR, "places_reviews.json")
    geojson_output = os.path.join(PROCESSED_GOOGLE_PLACES_DIR, "places_reviews.geojson")

    convert_places_json_to_geojson(json_input, geojson_output, city_name="Santo André")

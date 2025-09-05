"""
Neighborhoods Data Processing Pipeline for Santo André
======================================================

This script processes neighborhood-level data for Santo André, Brazil. 
It handles raw shapefiles, simplifies geometries, calculates centroids,
merges socio-economic indicators from SIDRA, and exports the results 
to both GeoJSON and CSV formats for downstream analysis.

Data Sources:
- Neighborhood shapefiles from SIGA
- Socio-economic indicators from SIDRA/IBGE (wide format CSV)

Main Steps:
1. Load and standardize neighborhood shapefile CRS.
2. Simplify polygons and calculate centroids.
3. Merge socio-economic indicators from SIDRA (wide format CSV).
4. Export processed neighborhoods to GeoJSON and CSV.
"""

import os
from typing import Optional, List
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon
from config.paths import RAW_SANTO_ANDRE_SIGA_DIR, PROCESSED_SANTO_ANDRE_SIGA_DIR, PROCESSED_SANTO_ANDRE_SIDRA_DIR

# ==============================
# FUNCTION: LOAD AND STANDARDIZE CRS
# ==============================

def load_and_standardize_crs(shapefile_path: str, target_epsg: int = 4674) -> gpd.GeoDataFrame:
    """
    Load a shapefile and ensure it uses the target CRS (default: EPSG:4674 - SIRGAS 2000).

    Parameters
    ----------
    shapefile_path : str
        Path to the shapefile.
    target_epsg : int, optional
        EPSG code to standardize the CRS (default is 4674).

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with standardized CRS.
    """
    print(f"Loading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)

    # Assign CRS if missing or reproject if necessary
    if gdf.crs is None:
        print(f"No CRS found. Assigning EPSG:{target_epsg}")
        gdf.set_crs(epsg=target_epsg, inplace=True)
    elif gdf.crs.to_epsg() != target_epsg:
        print(f"Converting CRS from {gdf.crs} to EPSG:{target_epsg}")
        gdf = gdf.to_crs(epsg=target_epsg)
    return gdf

# ==============================
# FUNCTION: PROCESS NEIGHBORHOODS
# ==============================

def process_neighborhoods(shapefile_path: str, tolerance: float) -> gpd.GeoDataFrame:
    """
    Process and simplify neighborhood polygons, compute centroids, and prepare WKT outputs.

    Parameters
    ----------
    shapefile_path : str
        Path to the neighborhood shapefile.
    tolerance : float
        Tolerance level for geometry simplification (e.g., 0.0003 ~ 30m).

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with simplified geometries, centroids, and WKT formats.
    """
    # Load and convert to WGS84
    gdf = load_and_standardize_crs(shapefile_path, target_epsg=4674).to_crs(epsg=4326)

    # Rename columns for consistency
    gdf = gdf.rename(columns={"ID": "id", "NOME": "name", "AREA": "area_km2"})

    # Simplify geometry
    print("Simplifying geometries...")
    gdf["geometry_simplified"] = gdf.geometry.simplify(tolerance, preserve_topology=True)

    # Compute centroids in projected CRS for better precision
    gdf_proj = gdf.to_crs(epsg=31983)
    gdf["centroid_geom"] = gdf_proj["geometry"].centroid
    gdf["centroid_geom"] = gdf.set_geometry("centroid_geom").to_crs(epsg=4326)["centroid_geom"]
    gdf["centroid_lat"] = gdf["centroid_geom"].y
    gdf["centroid_lon"] = gdf["centroid_geom"].x

    # Convert geometries to WKT and restore simplified geometry
    gdf["polygon_wkt"] = gdf["geometry_simplified"].apply(lambda g: g.wkt)
    gdf["centroid_wkt"] = gdf["centroid_geom"].apply(lambda g: g.wkt)
    gdf["geometry"] = gdf["polygon_wkt"].apply(wkt.loads)

    # Final GeoDataFrame
    return gpd.GeoDataFrame(
        gdf.drop(columns=["geometry_simplified", "centroid_geom"]),
        geometry="geometry",
        crs="EPSG:4326"
    )

# ==============================
# FUNCTION: EXPORT NEIGHBORHOODS
# ==============================

def export_neighborhoods(
    gdf: gpd.GeoDataFrame, 
    geojson_path: str, 
    csv_path: str, 
    geometry_column: str = "geometry"
) -> None:
    """
    Export processed neighborhood GeoDataFrame to GeoJSON and CSV formats.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        The GeoDataFrame to export.
    geojson_path : str
        Destination path for GeoJSON output.
    csv_path : str
        Destination path for CSV output.
    geometry_column : str, optional
        The column containing geometry information.
    """
    print("Validating geometries for export...")
    gdf_valid = gdf[gdf["geometry"].apply(lambda g: isinstance(g, (Polygon, MultiPolygon)))]

    # Select columns
    cols = [
        "neighborhood_id", "name", "area_km2", "average_monthly_income", "literacy_rate",
        "population_with_income", "total_literate_population", "total_private_households",
        "total_resident_population", "centroid_lat", "centroid_lon", geometry_column
    ]
    gdf_export = gdf_valid[cols].copy()
    gdf_export = gpd.GeoDataFrame(gdf_export, geometry="geometry", crs="EPSG:4326")
    gdf_export = gdf_export.sort_values(by="neighborhood_id")

    # Fill missing values
    gdf_export = gdf_export.fillna(0)

    # Export GeoJSON
    print(f"Exporting GeoJSON: {geojson_path}")
    os.makedirs(os.path.dirname(geojson_path), exist_ok=True)
    gdf_export.to_file(geojson_path, driver="GeoJSON")

    # Export CSV
    print(f"Exporting CSV: {csv_path}")
    gdf_export.drop(columns=["geometry"]).to_csv(csv_path, sep=';', index=False)
    print("Export finished.")

# ==============================
# FUNCTION: MERGE SIDRA DATA
# ==============================

def add_sidra_data_to_neighborhoods(
    gdf: gpd.GeoDataFrame, 
    sidra_wide_csv_path: str, 
    key_gdf: str = "id", 
    key_csv: str = "Neighborhood", 
    columns_to_add: Optional[List[str]] = None
) -> gpd.GeoDataFrame:
    """
    Merge SIDRA indicator data (wide format) into a neighborhoods GeoDataFrame.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame of neighborhoods.
    sidra_wide_csv_path : str
        Path to SIDRA data in wide format.
    key_gdf : str, optional
        Key column name in the GeoDataFrame.
    key_csv : str, optional
        Key column name in the CSV.
    columns_to_add : list or None
        Specific columns to include; if None, all are included.

    Returns
    -------
    geopandas.GeoDataFrame
        Updated GeoDataFrame with SIDRA data.
    """
    print(f"Reading SIDRA CSV: {sidra_wide_csv_path}")
    df_sidra = pd.read_csv(sidra_wide_csv_path, sep=';')

    # Select only the desired columns to merge
    if columns_to_add is None:
        # If no columns are specified, use all except the key
        columns_to_add = [col for col in df_sidra.columns if col != key_csv]
    
    # Filter the DataFrame to keep only the key and selected columns
    df_sidra = df_sidra[[key_csv] + columns_to_add]

    # Perform a left join between the GeoDataFrame and the SIDRA DataFrame
    print(f"Merging with neighborhoods on '{key_gdf}' = '{key_csv}'")
    gdf_merged = gdf.merge(df_sidra, how="left", left_on=key_gdf, right_on=key_csv)

    # Convert Neighborhood_ID to a nullable integer type (keeps NaN if needed)
    gdf_merged["Neighborhood_ID"] = gdf_merged["Neighborhood_ID"].astype("Int64")

    # Remove the redundant key column from the CSV if it differs from the GeoDataFrame key
    if key_csv != key_gdf:
        gdf_merged = gdf_merged.drop(columns=[key_csv])

    # Rename columns for consistency and easier downstream processing
    gdf_merged = gdf_merged.rename(columns={
        "Neighborhood_ID": "neighborhood_id",
        "Average_Monthly_Income": "average_monthly_income",
        "Literacy_Rate": "literacy_rate",
        "Population_with_Income": "population_with_income",
        "Total_Literate_Population": "total_literate_population",
        "Total_Private_Households": "total_private_households",
        "Total_Resident_Population": "total_resident_population"
    })

    print("Merge complete.")
    return gdf_merged

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    SHAPEFILE_PATH = os.path.join(RAW_SANTO_ANDRE_SIGA_DIR, "SIGA_LIM_BAIRROS_OFICIAL", "SIGA_LIM_BAIRROS_OFICIALPolygon.shp")
    GEOJSON_OUTPUT = os.path.join(PROCESSED_SANTO_ANDRE_SIGA_DIR, "neighborhoods_processed.geojson")
    CSV_OUTPUT = os.path.join(PROCESSED_SANTO_ANDRE_SIGA_DIR, "neighborhoods_processed.csv")
    SIDRA_WIDE_CSV = os.path.join(PROCESSED_SANTO_ANDRE_SIDRA_DIR, "neighborhoods_sidra_wide.csv")

    # Geometry simplification tolerance (~30 meters)
    GEOMETRY_SIMPLIFY_TOLERANCE = 0.0003

    # Process neighborhoods
    gdf = process_neighborhoods(SHAPEFILE_PATH, GEOMETRY_SIMPLIFY_TOLERANCE)

    # Merge socio-economic data
    gdf = add_sidra_data_to_neighborhoods(gdf, SIDRA_WIDE_CSV, key_gdf="name", key_csv="Neighborhood")

    # Export results
    export_neighborhoods(gdf, GEOJSON_OUTPUT, CSV_OUTPUT)
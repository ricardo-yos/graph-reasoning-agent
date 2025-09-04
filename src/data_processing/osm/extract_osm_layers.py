"""
OSM Data Extraction and Processing Pipeline
===========================================

This script extracts and processes OpenStreetMap (OSM) data for Santo André, Brazil.

Main steps:
1. Extract multiple OSM geometry layers (e.g., parks, pet shops, etc.).
2. Extract the complete street network (nodes and edges).
3. Filter intersections by street_count > 2.
4. Add unique road_id to street edges.
5. Save all layers into a single GeoPackage for further analysis.

Modules used:
- osmnx: for OSM data extraction
- geopandas: for geospatial data manipulation
- config: project-specific constants and paths
"""

import os
import osmnx as ox
import geopandas as gpd
from typing import Dict, Union, Optional, List, Tuple
from config.paths import PROCESSED_SANTO_ANDRE_OSM_DIR
from config.constants import LAYERS_TO_EXTRACT

# ==============================
# OSM DATA EXTRACTION
# ==============================

def extract_osm_data(
    place: str,
    tags: Dict[str, Union[str, bool]],
    data_type: str = "geometries"
) -> Union[gpd.GeoDataFrame, Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]]:
    """
    Extract data from OpenStreetMap for a given place and tag filters.

    Parameters
    ----------
    place : str
        The name of the location (e.g., "Santo André, São Paulo, Brazil").
    tags : dict
        A dictionary of OSM tag filters (e.g., {"shop": "pet"}).
    data_type : str, optional
        Either "geometries" to extract OSM features, or "graph" to extract the street network.

    Returns
    -------
    geopandas.GeoDataFrame
        If data_type="geometries", returns a GeoDataFrame with OSM features.

    tuple of (geopandas.GeoDataFrame, geopandas.GeoDataFrame)
        If data_type="graph", returns a tuple (nodes_gdf, edges_gdf)
        where nodes_gdf contains the street network nodes and 
        edges_gdf contains the street network edges.

    Raises
    ------
    ValueError
        If data_type is not "geometries" or "graph".
    """
    if data_type == "geometries":
        return ox.features_from_place(place, tags)
    elif data_type == "graph":
        graph = ox.graph_from_place(place, network_type="all")
        return ox.graph_to_gdfs(graph)
    else:
        raise ValueError("data_type must be 'geometries' or 'graph'.")

def extract_multiple_osm_layers(
    place: str,
    layers: List[Tuple[str, Dict[str, Union[str, bool]]]],
    output_gpkg_path: str
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Extract multiple layers from OSM and save them into a single GeoPackage file.

    Parameters
    ----------
    place : str
        The name of the place to query from OSM.
    layers : list of tuple
        A list of (layer_name, tag_dict) pairs.
    output_gpkg_path : str
        Path to the output .gpkg file.

    Returns
    -------
    dict
        A dictionary mapping each layer name to its corresponding GeoDataFrame.
    """
    results = {}
    for i, (layer_name, tags) in enumerate(layers):
        print(f"Extracting layer: {layer_name}")
        gdf = extract_osm_data(place, tags, data_type="geometries")
        results[layer_name] = gdf
        gdf.to_file(
            output_gpkg_path,
            layer=layer_name,
            driver="GPKG",
            mode="w" if i == 0 else "a"
        )
    return results

def extract_graph(place: str) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Extract the complete street network (nodes and edges) for a given place.

    Parameters
    ----------
    place : str
        The name of the place to query from OSM.

    Returns
    -------
    tuple of GeoDataFrame
        A tuple containing (nodes_gdf, edges_gdf).
    """
    nodes, edges = extract_osm_data(place, tags={}, data_type="graph")
    return nodes, edges

# ==============================
# MAIN PIPELINE EXECUTION
# ==============================

def run_pipeline():
    """
    Run the complete OSM data pipeline:
    1. Extract multiple OSM geometry layers.
    2. Extract the complete street graph (nodes + edges).
    3. Filter nodes with street_count > 2.
    4. Add a unique road_id to edges.
    5. Save all layers into a single GeoPackage file.
    """
    place_name = "Santo André, São Paulo, Brazil"
    output_gpkg = os.path.join(PROCESSED_SANTO_ANDRE_OSM_DIR, "santo_andre_osm_layers.gpkg")

    # Remove old GeoPackage file if it exists
    if os.path.exists(output_gpkg):
        os.remove(output_gpkg)

    # Step 1: Extract multiple geometry layers (e.g., parks, pet shops, etc.)
    osm_layers = extract_multiple_osm_layers(
        place=place_name,
        layers=LAYERS_TO_EXTRACT,
        output_gpkg_path=output_gpkg
    )

    # Step 2: Extract the complete street network (nodes and edges)
    print("Extracting full street network...")
    nodes_gdf, edges_gdf = extract_graph(place=place_name)

    # Reset index to convert MultiIndex (u, v, key) into columns
    edges_gdf = edges_gdf.reset_index()

    # Keep only selected edge attributes before saving
    columns_to_keep = [
        "u", "v", "key", "osmid", "highway", "oneway", "length", 
        "name", "maxspeed", "bridge", "tunnel", "geometry"
    ]
    edges_gdf = edges_gdf[[col for col in columns_to_keep if col in edges_gdf.columns]].copy()

    # Step 3: Add a unique road_id to each edge
    edges_gdf = edges_gdf.reset_index(drop=True)  # reset index to 0..N-1
    edges_gdf["road_id"] = edges_gdf.index.astype(str).map(lambda x: f"road_{x.zfill(6)}")

    # Step 4: Filter nodes with street_count > 2 if the column exists
    '''
    This keeps only intersections with more than two connected streets.
    Nodes with street_count <= 2 are usually dead-ends or simple segments,
    which are less relevant for routing and topological analysis.
    '''
    print("Filtering nodes by street_count > 2...")
    if "street_count" in nodes_gdf.columns:
        filtered_nodes = nodes_gdf[nodes_gdf["street_count"] > 2].copy()
    else:
        print("Warning: 'street_count' column not found in nodes layer. Skipping filtering.")
        filtered_nodes = nodes_gdf.copy()

    # Step 5: Save filtered nodes and edges to the GeoPackage
    filtered_nodes.to_file(output_gpkg, layer="nodes", driver="GPKG", mode="a")
    edges_gdf.to_file(output_gpkg, layer="edges", driver="GPKG", mode="a")

    # Update osm_layers dictionary
    osm_layers["nodes"] = filtered_nodes
    osm_layers["edges"] = edges_gdf

    print(f"\nAll layers successfully saved to:\n{output_gpkg}")

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    run_pipeline()

"""
Neo4j Pipeline for Santo André Data
===================================

Main pipeline for processing geographic and review data and inserting into Neo4j.

This script performs the ETL and graph-building workflow for Santo André data.
It loads datasets, assigns spatial relationships, creates indexes, inserts nodes,
and establishes relationships in the Neo4j graph.

Dependencies
------------
- pandas
- geopandas
- py2neo
- shapely (used by geopandas / spatial processing)
- config.env_loader
- config.paths
- graph.neo4j.connector
- graph.spatial.assign_spatial_attributes
- graph.neo4j.insert_nodes
- graph.neo4j.insert_relationships

Usage
-----
Run the script directly from the command line:

    python neo4j_pipeline.py

Notes
-----
- Ensure Neo4j is running and credentials are correctly set in environment variables.
- The script assumes all input datasets are prepared and contain required columns.
"""

import os
import pandas as pd
import geopandas as gpd
from config.env_loader import load_env, get_neo4j_credentials
from graph.neo4j.connector import Neo4jConnector
from graph.spatial.assign_spatial_attributes import SpatialRelator
from graph.neo4j.insert_nodes import Neo4jNodeInserter
from graph.neo4j.insert_relationships import Neo4jRelationInserter
from config.paths import PROCESSED_SANTO_ANDRE_SIGA_DIR, PROCESSED_SANTO_ANDRE_OSM_DIR, PROCESSED_GOOGLE_PLACES_DIR

# ----------------------------
# Main execution
# ----------------------------

def main() -> None:
    """
    Main pipeline to load data, assign spatial relationships,
    create indexes, insert nodes and relationships into Neo4j.

    Workflow:
    1. Load environment variables.
    2. Connect to Neo4j.
    3. Load geographic and review datasets.
    4. Assign spatial attributes linking places, roads, neighborhoods.
    5. Create Neo4j indexes.
    6. Insert nodes (neighborhoods, places, intersections, roads, reviews).
    7. Insert relationships among nodes.
    8. Print progress updates.
    """
    # Load environment variables from .env file
    load_env()
    url, user, password = get_neo4j_credentials()

    # Initialize Neo4j connector and get graph instance
    connector = Neo4jConnector(
        url=url,
        user=user,
        password=password
    )
    graph = connector.graph

    # Initialize spatial relator for assigning spatial attributes
    relator = SpatialRelator()

    print("Loading data...")
    neighborhoods = gpd.read_file(os.path.join(PROCESSED_SANTO_ANDRE_SIGA_DIR, "neighborhoods_processed.geojson"))
    places = gpd.read_file(os.path.join(PROCESSED_GOOGLE_PLACES_DIR, "places_reviews.geojson"))
    intersections = gpd.read_file(os.path.join(PROCESSED_SANTO_ANDRE_OSM_DIR, "santo_andre_osm_layers.gpkg"), layer="nodes")
    roads = gpd.read_file(os.path.join(PROCESSED_SANTO_ANDRE_OSM_DIR, "santo_andre_osm_layers.gpkg"), layer="edges")
    reviews = pd.read_csv(os.path.join(PROCESSED_GOOGLE_PLACES_DIR, "reviews_processed.csv"), sep=';')

    print("Assigning spatial relationships...")
    roads = relator.assign_roads_to_neighborhood(roads, neighborhoods)
    places = relator.assign_places_to_roads(places, roads)
    places = relator.assign_places_to_neighborhoods(places, neighborhoods)
    places = relator.assign_places_to_intersections(places, intersections)

    print("Creating indexes...")
    # Create Neo4j indexes for efficient querying
    connector.create_indexes()

    print("Inserting nodes...")
    # Initialize node inserter and insert different node types
    node_inserter = Neo4jNodeInserter(graph)
    node_inserter.insert_neighborhoods(neighborhoods)
    node_inserter.insert_places(places)
    node_inserter.insert_intersections(intersections)
    node_inserter.insert_roads(roads)
    node_inserter.insert_reviews(reviews)

    print("Inserting relationships...")
    # Initialize relationship inserter and insert relationships between nodes
    relation_inserter = Neo4jRelationInserter(graph)
    relation_inserter.insert_relationships(neighborhoods, places, roads, intersections, reviews)

    print("Done.")

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    main()
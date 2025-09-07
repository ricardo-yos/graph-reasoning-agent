"""
Neo4jRelationInserter Module
============================

This module provides the `Neo4jRelationInserter` class for creating
relationships between existing nodes in a Neo4j graph database. Supported
relationships include:

- Neighborhood CONTAINS Road
- Road CONTAINS Place
- Neighborhood CONTAINS Place
- Road connects Intersections (start 'u' → end 'v')
- Place NEAR Intersection (with distance)
- Place HAS_REVIEW Review

It is designed to work with GeoDataFrames for spatial data and pandas
DataFrames for reviews, ensuring all relationships are properly linked
based on unique identifiers.

Dependencies
------------
- pandas
- geopandas
- py2neo

Example
-------
from py2neo import Graph
from insert_relationships import Neo4jRelationInserter

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
relation_inserter = Neo4jRelationInserter(graph)
relation_inserter.insert_relationships(
    neighborhoods_df,
    places_gdf,
    roads_gdf,
    intersections_gdf,
    reviews_df
)
"""

import pandas as pd
import geopandas as gpd
from py2neo import Graph

class Neo4jRelationInserter:
    """
    Handles insertion of relationships between existing nodes in a Neo4j graph database.

    Parameters
    ----------
    graph : py2neo.Graph
        Neo4j graph database connection instance.
    """

    def __init__(self, graph: Graph) -> None:
        """
        Initialize the inserter with a Neo4j graph connection.

        Parameters
        ----------
        graph : py2neo.Graph
            Neo4j graph connection object used to execute Cypher queries.
        """
        self.graph = graph

    def insert_relationships(
        self,
        neighborhoods: gpd.GeoDataFrame,
        places: gpd.GeoDataFrame,
        roads: gpd.GeoDataFrame,
        intersections: gpd.GeoDataFrame | pd.DataFrame,
        reviews: pd.DataFrame,
    ) -> None:
        """
        Insert relationships between existing nodes in a Neo4j graph.

        Relationships include:
        - Neighborhoods containing roads and places
        - Roads connecting intersections and containing places
        - Places linked to nearest intersections and associated reviews

        Notes
        -----
        - Required columns must exist in the respective DataFrames; missing columns
          trigger a warning and skip the relationship.
        - Road 'u' and 'v' columns represent start and end intersections (osmid).
        - Distances are computed in meters if available.

        Parameters
        ----------
        neighborhoods : geopandas.GeoDataFrame
            GeoDataFrame containing neighborhoods with a 'neighborhood_id' column.
        places : geopandas.GeoDataFrame
            GeoDataFrame containing places with 'place_id', 'road_id', and 'neighborhood_id_osm' columns.
        roads : geopandas.GeoDataFrame
            GeoDataFrame containing roads with 'road_id', 'u', 'v', 'length', and optional attributes.
        intersections : pandas.DataFrame
            DataFrame containing intersections with 'osmid' as unique identifier.
        reviews : pandas.DataFrame
            DataFrame containing reviews with 'review_id' and 'place_id' for linking.
        """
        
        def valid_columns(df: pd.DataFrame | gpd.GeoDataFrame, required_cols: list[str]) -> bool:
            """
            Check if the required columns exist in a DataFrame or GeoDataFrame.

            Parameters
            ----------
            df : pandas.DataFrame or geopandas.GeoDataFrame
                The DataFrame or GeoDataFrame to check.
            required_cols : list of str
                List of column names that must be present in `df`.

            Returns
            -------
            bool
                True if all required columns exist in `df`, False otherwise.

            Notes
            -----
            - Prints a warning listing missing columns if any are not found.
            """
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                print(f"Warning: Missing columns {missing} in dataframe {type(df)}")
                return False
            return True

        # Neighborhood CONTAINS Road relationship
        if valid_columns(roads, ["neighborhood_id", "road_id"]):
            neighborhood_roads_rels = [
                {
                    "neighborhood_id": row["neighborhood_id"],
                    "road_id": row["road_id"],
                    "source": "neighborhood_to_road",
                }
                for _, row in roads.iterrows()
                if pd.notna(row["neighborhood_id"]) and pd.notna(row["road_id"])
            ]
            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (n:Neighborhood {neighborhood_id: row.neighborhood_id})
                MATCH (road:Road {road_id: row.road_id})
                MERGE (n)-[rel:CONTAINS]->(road)
                SET rel.source = row.source
                """,
                data=neighborhood_roads_rels,
            )

        # Road CONTAINS Place relationship
        if valid_columns(places, ["road_id", "place_id"]):
            road_place_rels = [
                {
                    "road_id": row["road_id"],
                    "place_id": row["place_id"],
                    "source": "georeferencing",
                }
                for _, row in places.iterrows()
                if pd.notna(row["road_id"]) and pd.notna(row["place_id"])
            ]
            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (r:Road {road_id: row.road_id})
                MATCH (p:Place {place_id: row.place_id})
                MERGE (r)-[rel:CONTAINS]->(p)
                SET rel.source = row.source
                """,
                data=road_place_rels,
            )

        # Neighborhood CONTAINS Place relationship
        if valid_columns(places, ["place_id", "neighborhood_id_osm"]):
            neighborhood_place_rels = [
                {
                    "place_id": row["place_id"],
                    "neighborhood_id": row["neighborhood_id_osm"],
                    "source": "neighborhood_contains_place",
                }
                for _, row in places.iterrows()
                if pd.notna(row["place_id"]) and pd.notna(row["neighborhood_id_osm"])
            ]
            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (n:Neighborhood {neighborhood_id: row.neighborhood_id})
                MATCH (p:Place {place_id: row.place_id})
                MERGE (n)-[r:CONTAINS]->(p)
                SET r.source = row.source
                """,
                data=neighborhood_place_rels,
            )

         # Intersection ROAD Intersection relationship
        if valid_columns(roads, ["road_id", "u", "v", "length"]):
            road_rels = [
                {
                    "road_id": row["road_id"],
                    "u": row["u"],
                    "v": row["v"],
                    "length": row["length"],
                    "oneway": row.get("oneway", False),
                    "name": row.get("name", None),
                    "maxspeed": row.get("maxspeed", None),
                }
                for _, row in roads.iterrows()
                if pd.notna(row["road_id"]) and pd.notna(row["u"]) and pd.notna(row["v"])
            ]

            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (start:Intersection {osmid: row.u})
                MATCH (end:Intersection {osmid: row.v})
                MERGE (start)-[r:ROAD]->(end)
                SET r.road_id = row.road_id,
                    r.length = row.length,
                    r.oneway = row.oneway,
                    r.name = row.name,
                    r.maxspeed = row.maxspeed
                WITH start, end, row
                WHERE row.oneway = false OR row.oneway IS NULL
                MERGE (end)-[r2:ROAD]->(start)
                SET r2.road_id = row.road_id,
                    r2.length = row.length,
                    r2.oneway = row.oneway,
                    r2.name = row.name,
                    r2.maxspeed = row.maxspeed
                """,
                data=road_rels,
            )

        # Place NEAR Intersection relationship
        if valid_columns(places, ["place_id", "nearest_intersection_osmid", "distance_to_nearest_intersection_m"]):
            place_intersection_rels = [
                {
                    "place_id": row["place_id"],
                    "intersection_osmid": row["nearest_intersection_osmid"],
                    "distance_m": row["distance_to_nearest_intersection_m"],
                    "source": "place_to_nearest_intersection",
                }
                for _, row in places.iterrows()
                if pd.notna(row["place_id"]) and pd.notna(row["nearest_intersection_osmid"])
            ]
            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (p:Place {place_id: row.place_id})
                MATCH (i:Intersection {osmid: row.intersection_osmid})
                MERGE (p)-[rel:NEAR]->(i)
                SET rel.distance_m = row.distance_m,
                    rel.source = row.source
                """,
                data=place_intersection_rels,
            )

        # Place HAS_REVIEW Review relationship
        if valid_columns(reviews, ["review_id", "place_id"]):
            review_place_rels = [
                {
                    "review_id": row["review_id"],
                    "place_id": row["place_id"],
                    "source": "place_to_review",
                }
                for _, row in reviews.iterrows()
                if pd.notna(row["review_id"]) and pd.notna(row["place_id"])
            ]
            self.graph.run(
                """
                UNWIND $data AS row
                MATCH (r:Review {review_id: row.review_id})
                MATCH (p:Place {place_id: row.place_id})
                MERGE (p)-[rel:HAS_REVIEW]->(r)
                SET rel.source = row.source
                """,
                data=review_place_rels,
            )
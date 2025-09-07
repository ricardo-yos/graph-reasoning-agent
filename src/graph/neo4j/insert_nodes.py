"""
Neo4jNodeInserter Module
========================

This module provides the `Neo4jNodeInserter` class for inserting nodes
into a Neo4j graph database. It includes methods to insert different
types of nodes such as neighborhoods, places, roads, intersections, and
reviews. Each method prepares data from pandas or GeoPandas DataFrames
and performs batch insertion using Cypher queries.

Dependencies
------------
- pandas
- geopandas
- py2neo

Example
-------
from insert_nodes import Neo4jNodeInserter
from py2neo import Graph

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
node_inserter = Neo4jNodeInserter(graph)

node_inserter.insert_neighborhoods(neighborhoods_df)
node_inserter.insert_places(places_df)
node_inserter.insert_roads(roads_gdf)
node_inserter.insert_intersections(intersections_gdf)
node_inserter.insert_reviews(reviews_df)
"""

import pandas as pd
import geopandas as gpd
from py2neo import Graph

class Neo4jNodeInserter:
    """
    Handles insertion of nodes into a Neo4j graph database.

    Parameters
    ----------
    graph : neo4j.Graph
        Instance of the Neo4j graph database connection/session used to run Cypher queries.
    """

    def __init__(self, graph: Graph) -> None:
        """
        Initialize the Neo4jNodeInserter with a Neo4j graph connection.

        Parameters
        ----------
        graph : py2neo.Graph
            Instance of the Neo4j graph database connection/session used to run Cypher queries.
        """
        self.graph = graph

    def insert_neighborhoods(self, neighborhoods: pd.DataFrame) -> None:
        """
        Insert neighborhood nodes into the Neo4j graph.

        Each node represents a neighborhood with socio-demographic properties
        and geographic centroid coordinates.

        Parameters
        ----------
        neighborhoods : pandas.DataFrame
            DataFrame with columns such as:
            'neighborhood_id', 'name', 'area_km2', 'average_monthly_income',
            'literacy_rate', 'population_with_income', 'total_literate_population',
            'total_private_households', 'total_resident_population', 'centroid_lat', 'centroid_lon'.
        """

        data = [{
            "neighborhood_id": row["neighborhood_id"],
            "name": row.get("name"),
            "area_km2": row.get("area_km2"),
            "average_monthly_income": row.get("average_monthly_income"),
            "literacy_rate": row.get("literacy_rate"),
            "population_with_income": row.get("population_with_income"),
            "total_literate_population": row.get("total_literate_population"),
            "total_private_households": row.get("total_private_households"),
            "total_resident_population": row.get("total_resident_population"),
            "centroid_lat": row.get("centroid_lat"),
            "centroid_lon": row.get("centroid_lon")
        } for _, row in neighborhoods.iterrows() if pd.notna(row.get("neighborhood_id"))]

        self.graph.run("""
            UNWIND $data AS row
            MERGE (n:Neighborhood {neighborhood_id: row.neighborhood_id})
            SET n.name = row.name,
                n.area_km2 = row.area_km2,
                n.average_monthly_income = row.average_monthly_income,
                n.literacy_rate = row.literacy_rate,
                n.population_with_income = row.population_with_income,
                n.total_literate_population = row.total_literate_population,
                n.total_private_households = row.total_private_households,
                n.total_resident_population = row.total_resident_population,
                n.centroid_lat = row.centroid_lat,
                n.centroid_lon = row.centroid_lon
        """, data=data)

    def insert_places(self, places: pd.DataFrame) -> None:
        """
        Insert place nodes into the Neo4j graph.

        Each node represents a place (e.g., business or point of interest) with attributes
        including name, rating, number of reviews, type, and geographic coordinates.

        Parameters
        ----------
        places : pandas.DataFrame
            DataFrame with columns:
            'place_id', 'name', 'rating', 'num_reviews', 'type', 'latitude', 'longitude'.
        """

        data = [{
            "place_id": row["place_id"],
            "name": row.get("name"),
            "rating": row.get("rating"),
            "num_reviews": row.get("num_reviews"),
            "type": row.get("type"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude")
        } for _, row in places.iterrows() if pd.notna(row.get("place_id"))]

        self.graph.run("""
            UNWIND $data AS row
            MERGE (p:Place {place_id: row.place_id})
            SET p.name = row.name,
                p.rating = row.rating,
                p.num_reviews = row.num_reviews,
                p.type = row.type,
                p.latitude = row.latitude,
                p.longitude = row.longitude
        """, data=data)

    def insert_roads(self, roads: gpd.GeoDataFrame) -> None:
        """
        Insert road nodes into the Neo4j graph.

        Each road node includes attributes such as unique road identifier, name, highway type,
        length, oneway flag, maximum speed, and connectivity information ('u', 'v', 'osmid').

        Parameters
        ----------
        roads : geopandas.GeoDataFrame
            GeoDataFrame containing road edges with columns:
            'road_id', 'name', 'highway', 'length', 'oneway', 'maxspeed', 'u', 'v', 'osmid'.
        """

        data = [{
            "road_id": row["road_id"],
            "name": row["name"] if pd.notna(row["name"]) else "",
            "highway": row["highway"] if pd.notna(row["highway"]) else "",
            "length": float(row["length"]) if pd.notna(row["length"]) else 0.0,
            "oneway": (str(row["oneway"]).lower() in ("true", "1", "yes")) if pd.notna(row["oneway"]) else False,
            "maxspeed": row["maxspeed"] if pd.notna(row["maxspeed"]) else "",
            "u": int(row["u"]) if pd.notna(row.get("u")) else None,
            "v": int(row["v"]) if pd.notna(row.get("v")) else None,
            "osmid": row.get("osmid", None)
        } for _, row in roads.iterrows() if pd.notna(row.get("road_id"))]

        self.graph.run("""
            UNWIND $data AS row
            MERGE (r:Road {road_id: row.road_id})
            SET r.name = row.name,
                r.highway = row.highway,
                r.length = row.length,
                r.oneway = row.oneway,
                r.maxspeed = row.maxspeed,
                r.u = row.u,
                r.v = row.v,
                r.osmid = row.osmid
        """, data=data)

    def insert_intersections(self, intersections: gpd.GeoDataFrame) -> None:
        """
        Insert intersection nodes into the Neo4j graph.

        Each intersection node contains an 'osmid' identifier, latitude and longitude,
        highway classification, and the number of streets connecting at the intersection.

        Parameters
        ----------
        intersections : geopandas.GeoDataFrame
            GeoDataFrame with columns:
            'osmid', 'geometry' (shapely Point), 'highway', 'street_count'.
        """

        data = [{
            "osmid": row["osmid"],
            "lat": row.geometry.y,
            "lon": row.geometry.x,
            "highway": row["highway"] if pd.notna(row.get("highway")) else "",
            "street_count": int(row["street_count"]) if pd.notna(row.get("street_count")) else 0
        } for _, row in intersections.iterrows()
          if pd.notna(row.get("osmid"))
          and hasattr(row, "geometry")
          and row.geometry.geom_type == 'Point'
        ]

        self.graph.run("""
            UNWIND $data AS row
            MERGE (i:Intersection {osmid: row.osmid})
            SET i.lat = row.lat,
                i.lon = row.lon,
                i.highway = row.highway,
                i.street_count = row.street_count
        """, data=data)

    def insert_reviews(self, reviews: pd.DataFrame) -> None:
        """
        Insert review nodes into the Neo4j graph.

        Each review node has properties such as review ID, author, rating, text content,
        and optionally the review date.

        Parameters
        ----------
        reviews : pandas.DataFrame
            DataFrame containing review data with columns:
            'review_id', 'author', 'rating', 'text', and optionally 'date'.
            If 'date' is missing or NaN, the property will be set to None.
        """
        
        data = [{
            "review_id": row["review_id"],
            "author": row.get("author"),
            "rating": row.get("rating"),
            "text": row.get("text"),
            "date": row.get("date"),
        } for _, row in reviews.iterrows() if pd.notna(row.get("review_id"))]

        self.graph.run("""
            UNWIND $data AS row
            MERGE (r:Review {review_id: row.review_id})
            SET r.author = row.author,
                r.rating = row.rating,
                r.text = row.text,
                r.date = row.date
        """, data=data)
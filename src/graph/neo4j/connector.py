"""
Neo4jConnector Module
=====================

This module provides the `Neo4jConnector` class for managing connections
to a Neo4j graph database and creating indexes on key properties for
efficient querying.

It can be used as a reusable component in pipelines or scripts that
interact with Neo4j, including inserting nodes, creating relationships,
and executing queries.

Dependencies
------------
- py2neo

Example
-------
from connector import Neo4jConnector

connector = Neo4jConnector(url="bolt://localhost:7687", user="neo4j", password="password")
connector.create_indexes()
"""

from py2neo import Graph

class Neo4jConnector:
    """
    Neo4j database connector class that manages connection and index creation
    for key properties in the graph.

    Parameters
    ----------
    url : str
        URI for connecting to the Neo4j database (e.g., "bolt://localhost:7687").
    user : str
        Username for Neo4j authentication.
    password : str
        Password for Neo4j authentication.
    """

    def __init__(self, url: str, user: str, password: str) -> None:
        """
        Initialize the Neo4jConnector with connection parameters.

        Parameters
        ----------
        url : str
            The Neo4j database URI.
        user : str
            Username for authentication.
        password : str
            Password for authentication.
        """
        self.graph = Graph(url, auth=(user, password))

    def create_indexes(self) -> None:
        """
        Create indexes on key properties for faster querying.

        Indexes are created on:
        - Place.place_id
        - Neighborhood.neighborhood_id
        - Road.road_id
        - Intersection.osmid
        - Review.review_id

        Uses 'IF NOT EXISTS' to avoid errors if indexes already exist.
        """
        self.graph.run(
            "CREATE INDEX place_id_index IF NOT EXISTS FOR (p:Place) ON (p.place_id)"
        )
        self.graph.run(
            "CREATE INDEX neighborhood_id_index IF NOT EXISTS FOR (n:Neighborhood) ON (n.neighborhood_id)"
        )
        self.graph.run(
            "CREATE INDEX road_id_index IF NOT EXISTS FOR (r:Road) ON (r.road_id)"
        )
        self.graph.run(
            "CREATE INDEX intersection_osmid_index IF NOT EXISTS FOR (i:Intersection) ON (i.osmid)"
        )
        self.graph.run(
            "CREATE INDEX review_id_index IF NOT EXISTS FOR (r:Review) ON (r.review_id)"
        )
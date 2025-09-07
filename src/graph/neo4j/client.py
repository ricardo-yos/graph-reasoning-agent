"""
Neo4j Client Module
===================

This module provides the Neo4jClient class, a simple wrapper for connecting
to a Neo4j database, executing Cypher queries, and managing the driver session.

It handles:
- Driver initialization and connection management
- Running Cypher queries with optional parameters
- Returning query results as a list of dictionaries

Dependencies
------------
- neo4j
- config.env_loader

Usage
-----
Example usage:

    from client import Neo4jClient
    from config.env_loader import load_env, get_neo4j_credentials

    load_env()
    uri, user, password = get_neo4j_credentials()
    client = Neo4jClient(uri, user, password)
    results = client.run_query("MATCH (n) RETURN n LIMIT 10")
    client.close()
"""

from typing import Any, Dict, List, Optional
from neo4j import GraphDatabase, basic_auth
from config.env_loader import load_env, get_neo4j_credentials

class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str) -> None:
        """
        Initialize the Neo4j driver connection.

        Parameters
        ----------
        uri : str
            Neo4j connection URI, e.g., "bolt://localhost:7687"
        user : str
            Username for Neo4j authentication
        password : str
            Password for Neo4j authentication
        """
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))

    def close(self) -> None:
        """
        Close the Neo4j driver connection.
        """
        self.driver.close()

    def run_query(self, cypher_query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return the results.

        Parameters
        ----------
        cypher_query : str
            Cypher query string
        parameters : dict, optional
            Parameters for the Cypher query, by default None

        Returns
        -------
        list of dict
            List of records as dictionaries with key-value pairs for each returned variable.
        """
        with self.driver.session() as session:
            result = session.run(cypher_query, parameters or {})
            return [record.data() for record in result]
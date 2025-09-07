"""
Neo4j Connection Tester
======================

This module provides a utility function `neo4j_tester` to test the
connection to a Neo4j database. It loads environment variables,
initializes the Neo4jClient, executes a sample query, prints the results,
and closes the connection.

Dependencies
------------
- neo4j
- config.env_loader

Usage
-----
Example:

    from client_tester import neo4j_tester

    neo4j_tester()
"""

from typing import List, Dict
from config.env_loader import load_env, get_neo4j_credentials
from graph.neo4j.client import Neo4jClient

def neo4j_tester() -> None:
    """
    Test connection to Neo4j and execute a sample query.

    The function:
    1. Loads environment variables for Neo4j credentials.
    2. Initializes the Neo4jClient.
    3. Executes a sample query to fetch places of type 'pet_store' in a neighborhood named 'Jardim'.
    4. Prints the results.
    5. Closes the client connection.

    Notes
    -----
    - Make sure environment variables (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) are set.
    """
    # Load environment variables (like NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    load_env()
    
    # Get Neo4j credentials from environment or config
    uri, user, password = get_neo4j_credentials()

    # Initialize Neo4j client
    client = Neo4jClient(uri, user, password)

    # Sample query: fetch pet stores in the 'Jardim' neighborhood
    query = """
    MATCH (n:Neighborhood)-[:CONTAINS]->(pl:Place)
    WHERE n.name = 'Jardim' AND pl.type = 'pet_store'
    RETURN pl.name
    """

    # Execute query
    results: List[Dict] = client.run_query(query)

    # Print results
    print(results)

    # Close the connection
    client.close()

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    neo4j_tester()
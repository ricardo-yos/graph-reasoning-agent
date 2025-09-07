"""
Neo4j Batch Deletion Script
=========================== 

This script provides a utility to delete all nodes and relationships
from a Neo4j database in batches, which helps prevent connection errors
when handling large datasets.

Dependencies
------------
- py2neo
- config.env_loader
- other required packages...

Usage
-----
Run as a standalone script:

    python neo4j_deletions.py

Notes
-----
- Ensure Neo4j is running and accessible at the configured URI.
- Environment variables NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set.
"""

from py2neo import Graph
from config.env_loader import load_env, get_neo4j_credentials
from graph.neo4j.connector import Neo4jConnector

def delete_all_in_batches(graph: Graph, batch_size: int = 10000) -> None:
    """
    Delete all nodes and relationships from Neo4j in batches to avoid connection errors.

    Parameters
    ----------
    graph : Graph
        Neo4j graph database connection instance.
    batch_size : int, optional
        Number of nodes to delete per batch (default is 10,000).

    Returns
    -------
    None
    """
    total_deleted = 0
    while True:
        # Delete nodes in batches using Cypher and return the number deleted
        result = graph.run(f"""
            MATCH (n)
            WITH n LIMIT {batch_size}
            DETACH DELETE n
            RETURN count(n) AS deleted_count
        """).data()

        deleted = result[0]["deleted_count"] if result else 0
        total_deleted += deleted

        print(f"Deleted {deleted} nodes in this batch. Total deleted: {total_deleted}")

        # Stop when no more nodes are left to delete
        if deleted == 0:
            print("No more nodes to delete. Finished.")
            break

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    # Load environment variables from .env file
    load_env()

    # Retrieve Neo4j credentials from environment variables
    url, user, password = get_neo4j_credentials()

    # Initialize Neo4j connector and get the graph instance
    connector = Neo4jConnector(
        url=url,
        user=user,
        password=password
    )

    # Delete all nodes in batches
    graph = connector.graph
    delete_all_in_batches(graph)
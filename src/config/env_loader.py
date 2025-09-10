"""
Environment Loader Utility
==========================

This module provides utility functions to load environment variables
from a `.env` file and retrieve Neo4j connection credentials.

Functions
---------
- load_env()
    Loads environment variables from the `.env` file located at ENV_PATH.
- get_neo4j_credentials()
    Retrieves Neo4j connection parameters (URI, username, password)
    from environment variables.

Dependencies
------------
- python-dotenv
- config.paths (ENV_PATH)

Example
-------
from config.env_loader import load_env, get_neo4j_credentials

# Load environment variables
load_env()

# Get Neo4j credentials
url, user, password = get_neo4j_credentials()
print(url, user, password)
"""

import os
from dotenv import load_dotenv
from config.paths import ENV_PATH

def load_env():
    """
    Load environment variables from a .env file into the system environment.

    This function uses python-dotenv to read variables from the `.env` file
    located at the path specified by ENV_PATH.
    """
    load_dotenv(ENV_PATH)

def get_neo4j_credentials():
    """
    Retrieve Neo4j connection credentials from environment variables.

    Returns
    -------
    tuple
        A tuple (url, user, password) containing the Neo4j connection parameters.
        Each element is a string or None if the variable is not set.

    Expected environment variables
    ------------------------------
    - NEO4J_URI : str
        Neo4j database connection URL.
    - NEO4J_USERNAME : str
        Username for Neo4j authentication.
    - NEO4J_PASSWORD : str
        Password for Neo4j authentication.
    """
    url = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    return url, user, password
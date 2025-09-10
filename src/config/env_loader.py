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

def load_env() -> None:
    """
    Load environment variables from a .env file into the system environment.

    This function reads all variables from the `.env` file located at
    the path specified by ENV_PATH and loads them into os.environ.

    Raises
    ------
    FileNotFoundError
        If the `.env` file does not exist at ENV_PATH.
    """
    if not os.path.exists(ENV_PATH):
        raise FileNotFoundError(f".env file not found at path: {ENV_PATH}")
    load_dotenv(ENV_PATH)

def get_neo4j_credentials() -> tuple[str, str, str]:
    """
    Retrieve Neo4j connection credentials from environment variables.

    This function assumes that the environment variables are already loaded
    (e.g., by calling `load_env()`).

    Returns
    -------
    tuple[str, str, str]
        A tuple (url, user, password) containing the Neo4j connection parameters.

    Raises
    ------
    KeyError
        If any of the required environment variables (NEO4J_URI, NEO4J_USERNAME,
        NEO4J_PASSWORD) are missing.
    """
    url: str = os.environ["NEO4J_URI"]
    user: str = os.environ["NEO4J_USERNAME"]
    password: str = os.environ["NEO4J_PASSWORD"]

    return url, user, password

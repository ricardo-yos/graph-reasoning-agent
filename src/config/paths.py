"""
Graph Reasoning Project Directory Constants
===========================================

Centralizes all important filesystem paths used across the project, including:
- Raw, interim, and processed data directories
- Logs and models storage
- Vector databases for RAG and graph navigation

This ensures consistent path management and simplifies access to project resoaaaaaurces.
"""

import os

# Root directory (four levels above the current file)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Top-level directories
DATA_DIR = os.path.join(ROOT_DIR, "data")           # Main data directory
LOGS_DIR = os.path.join(ROOT_DIR, "logs")           # Logs directory
MODELS_DIR = os.path.join(ROOT_DIR, "models")       # Machine learning models
VECTOR_DB_DIR = os.path.join(ROOT_DIR, "chromadb")  # Chroma vector databases

# Data subdirectories
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")        # Raw/unprocessed data
INTERIM_DATA_DIR = os.path.join(DATA_DIR, "interim")# Intermediate/temporary data
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed") # Cleaned/processed data
RAG_DATA_DIR = os.path.join(DATA_DIR, "rag")        # RAG-related data
FUZZY_DATA_DIR = os.path.join(DATA_DIR, "fuzzy")    # Fuzzy-matching data

# Raw data directories organized by source
RAW_GOOGLE_PLACES_DIR = os.path.join(RAW_DATA_DIR, "google_places")   # Google Places raw data
RAW_SANTO_ANDRE_OSM_DIR = os.path.join(RAW_DATA_DIR, "santo_andre_osm") # OSM raw data for Santo André
RAW_SANTO_ANDRE_SIDRA_DIR = os.path.join(RAW_DATA_DIR, "santo_andre_sidra_ibge") # SIDRA raw data
RAW_SANTO_ANDRE_SIGA_DIR = os.path.join(RAW_DATA_DIR, "santo_andre_siga") # SIGA raw data

# Interim data directories organized by source
INTERIM_SANTO_ANDRE_SIDRA_DIR = os.path.join(INTERIM_DATA_DIR, "santo_andre_sidra_ibge") # Interim SIDRA data

# Processed data directories organized by source
PROCESSED_GOOGLE_PLACES_DIR = os.path.join(PROCESSED_DATA_DIR, "google_places") # Processed Google Places data
PROCESSED_SANTO_ANDRE_OSM_DIR = os.path.join(PROCESSED_DATA_DIR, "santo_andre_osm") # Processed OSM data
PROCESSED_SANTO_ANDRE_SIDRA_DIR = os.path.join(PROCESSED_DATA_DIR, "santo_andre_sidra_ibge") # Processed SIDRA data
PROCESSED_SANTO_ANDRE_SIGA_DIR = os.path.join(PROCESSED_DATA_DIR, "santo_andre_siga") # Processed SIGA data
JSON_PATH = os.path.join(PROCESSED_DATA_DIR, "places_reviews.json") # Combined reviews JSON file

# Chroma vector database directories
VECTOR_DB_CYPHER_RAG = os.path.join(VECTOR_DB_DIR, "cypher_rag")       # Chroma DB for Cypher RAG
VECTOR_DB_GRAPH_NAVIGATOR = os.path.join(VECTOR_DB_DIR, "graph_navigator") # Chroma DB for Graph Navigator

# Configuration files
ENV_PATH = os.path.join(ROOT_DIR, ".env") # Environment variables file
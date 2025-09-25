"""
Vector Database Builder for Cypher RAG
======================================

This module builds a Chroma vector database from a JSON file containing
Cypher RAG training examples (questions, intentions, and Cypher queries).

Features
--------
- Converts natural language questions into embeddings using HuggingFace models.
- Persists the embeddings and metadata (question, intention, Cypher) into Chroma.
- Deletes the existing collection before rebuilding.

Dependencies
------------
- os, json (standard library)
- chromadb
- langchain_huggingface
- config.paths (custom module providing RAG_DATA_DIR and VECTOR_DB_CYPHER_RAG)

Expected JSON Format
--------------------
The JSON file must be a list of objects with the following structure:
[
    {
        "question": "string",
        "intention": "string",
        "cypher": "string"
    },
    ...
]

Usage
-----
Run the script directly to build the vector database:

    $ python build_vector_db.py

This will:
1. Read the input file located at `RAG_DATA_DIR/rag_questions_cypher.json`
2. Build embeddings for each question
3. Persist the vector database in `VECTOR_DB_CYPHER_RAG`
"""

import os
import json
import chromadb
from typing import List, Dict, Any
from langchain_huggingface import HuggingFaceEmbeddings
from config.paths import RAG_DATA_DIR, VECTOR_DB_CYPHER_RAG

def build_vector_db(json_filepath: str, persist_path: str) -> None:
    """
    Build a Chroma vector database from a JSON file containing
    question, intention, and Cypher examples.

    Parameters
    ----------
    json_filepath : str
        Path to the JSON file with "question", "intention", and "cypher" fields.
    persist_path : str
        Directory path where the Chroma DB will be persisted.

    Notes
    -----
    - If a previous collection exists, it will be deleted before rebuilding.
    """
    # Load the dataset from JSON
    with open(json_filepath, encoding="utf-8") as f:
        data: List[Dict[str, Any]] = json.load(f)

    # Initialize multilingual embeddings
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/distiluse-base-multilingual-cased-v2"
    )

    # Initialize Chroma client with persistence
    client = chromadb.PersistentClient(path=persist_path)

    # Reset collection if it exists
    collection_name = "rag_cypher"
    existing_collections = [c.name for c in client.list_collections()]
    if collection_name in existing_collections:
        print(f"Deleting existing collection '{collection_name}'...")
        client.delete_collection(name=collection_name)

    # Create new collection
    collection = client.get_or_create_collection(name=collection_name)

    # Iterate through dataset entries and insert them into Chroma
    for i, entry in enumerate(data):
        question: str = entry["question"]
        intention: str = entry["intention"]
        cypher: str = entry["cypher"]

        # Generate embedding for the question
        vector: List[float] = embeddings.embed_query(question)

        # Store embedding, question text, intention, and Cypher query as metadata
        collection.add(
            ids=[str(i)],
            embeddings=[vector],
            documents=[question],
            metadatas=[{
                "question": question,
                "intention": intention,
                "cypher": cypher
            }],
        )

    print(f"Vector DB created at '{persist_path}' with {len(data)} entries.")

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    json_filepath: str = os.path.join(RAG_DATA_DIR, "rag_questions_cypher.json")
    persist_path: str = VECTOR_DB_CYPHER_RAG  # export vector DB to VECTOR_DB_CYPHER_RAG

    build_vector_db(json_filepath, persist_path)

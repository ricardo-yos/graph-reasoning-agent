"""
Chroma Vector Database Tester for Cypher RAG
============================================

This module allows testing retrieval from a Chroma vector database
containing Cypher RAG examples (questions, intentions, and Cypher queries).

It uses HuggingFace embeddings to encode a query and performs a similarity
search in the Chroma vector database.

Dependencies
------------
- os (standard library)
- langchain_huggingface
- langchain_chroma
- config.paths (custom module providing VECTOR_DB_DIR)

Usage
-----
Run the script directly to test vector DB retrieval:

    $ python test_chroma_vector_db.py

You can modify `sample_query` in the entry point to test different queries.
"""

import os
from typing import List
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from config.paths import VECTOR_DB_DIR

def test_chroma_vector_db(query_text: str, top_k: int = 2) -> None:
    """
    Test retrieval from Chroma vector database for a given query.

    Parameters
    ----------
    query_text : str
        The query string to search in the vector DB.
    top_k : int, optional
        Number of top similar results to return. Default is 2.

    Returns
    -------
    None
        Prints the top K results with their Cypher query and text.
    """
    # Initialize embedding model
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/distiluse-base-multilingual-cased-v2"
    )

    # Connect to Chroma vector DB
    collection = Chroma(
        collection_name="rag_cypher",
        embedding_function=embeddings,
        persist_directory=VECTOR_DB_DIR
    )

    # Perform the similarity search
    results = collection.similarity_search(query_text, k=top_k)

    print(f"Query: {query_text}")
    print(f"Top {top_k} results:")

    for i, doc in enumerate(results, 1):
        print(f"\nResult {i}:")
        print(f"Cypher: {doc.metadata.get('cypher', 'N/A')}")
        print(f"Text: {doc.page_content}")

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    sample_query = "Quais os nomes dos petshops do bairro Vila Pires?"
    test_chroma_vector_db(sample_query)

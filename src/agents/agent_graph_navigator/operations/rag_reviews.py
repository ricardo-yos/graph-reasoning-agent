"""
Review RAG Retriever with Place Filtering
=========================================

This module provides functionality to query a ChromaDB vector store
of review embeddings and expand reviews for filtered Places using 
a Retrieval-Augmented Generation (RAG) approach.

Features
--------
- Query ChromaDB with a text input.
- Filter review chunks by `place_id` before similarity ranking.
- Compute cosine similarity between query and embeddings.
- Expand reviews for RAG queries only for valid Places.

Dependencies
------------
- typing
- torch
- numpy
- langchain_huggingface
- langchain_chroma
- config.paths

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.rag_reviews import expand_reviews_with_rag

    # RAG queries
    extracted_nodes = {"RAG": [{"text": "banho e tosa"}]}

    # Filtered Places
    result = {"Place": [{"attributes": {"place_id": "ChIJawigxnFCzpQRQW_XZ4K1ph8"}}]}

    # Expand reviews
    rag_reviews = expand_reviews_with_rag(extracted_nodes, result, top_k=5)
"""

from typing import List, Dict, Any
import torch
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from config.paths import VECTOR_DB_GRAPH_NAVIGATOR
import numpy as np

# -----------------------------
# Query ChromaDB and filter by place_id
# -----------------------------

def query_reviews_filtered(
    query_text: str,
    valid_place_ids: set,
    top_k: int = 5,
    embedding_model_name: str = "neuralmind/bert-base-portuguese-cased"
) -> List[Dict[str, Any]]:
    """
    Query ChromaDB for review chunks and filter results by `place_id`.

    This function retrieves all stored chunks from the Chroma collection,
    filters them based on the set of valid `place_id`s, computes cosine
    similarity between the query and the chunk embeddings, and returns
    the top_k most similar chunks.

    Parameters
    ----------
    query_text : str
        Input text to query ChromaDB.
    valid_place_ids : set
        Set of allowed place IDs to filter results.
    top_k : int, default=5
        Number of most similar chunks to return.
    embedding_model_name : str
        HuggingFace model name for embedding computation.

    Returns
    -------
    List[dict]
        List of dictionaries containing:
        - 'text': chunk text
        - 'metadata': chunk metadata (review_id, place_id, place_name, etc.)
        - 'score': cosine similarity score
    """
    # Select device for embeddings
    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Initialize HuggingFace embeddings
    embedder = HuggingFaceEmbeddings(
        model_name=embedding_model_name,
        model_kwargs={"device": device}
    )

    # Connect to Chroma collection
    collection = Chroma(
        collection_name="review_embeddings",
        embedding_function=embedder,
        persist_directory=VECTOR_DB_GRAPH_NAVIGATOR
    )

    # Retrieve all stored documents, metadata, and embeddings
    stored = collection.get(include=["documents", "metadatas", "embeddings"])
    documents = stored.get("documents", [])
    metadatas = stored.get("metadatas", [])
    embeddings = stored.get("embeddings", [])

    # Filter chunks by valid place_ids
    filtered_chunks = [
        (doc, meta, emb)
        for doc, meta, emb in zip(documents, metadatas, embeddings)
        if str(meta.get("place_id")).strip() in valid_place_ids
    ]

    if not filtered_chunks:
        return []

    # Separate filtered data
    chunk_texts, chunk_metas, chunk_embs = zip(*filtered_chunks)
    chunk_embs = np.array(chunk_embs)

    # Compute embedding for the query
    query_emb = np.array(embedder.embed_documents([query_text]))[0]

    # Compute cosine similarity
    scores = chunk_embs @ query_emb / (np.linalg.norm(chunk_embs, axis=1) * np.linalg.norm(query_emb) + 1e-10)

    # Get top_k indices by similarity
    top_indices = np.argsort(-scores)[:top_k]

    # Prepare results
    results = []
    for idx in top_indices:
        results.append({
            "text": chunk_texts[idx],
            "metadata": chunk_metas[idx],
            "score": float(scores[idx])
        })
    return results

# -----------------------------
# Expand reviews using RAG
# -----------------------------

def expand_reviews_with_rag(
    extracted_nodes: dict,
    result: dict,
    top_k: int = 5
) -> List[Dict[str, str]]:
    """
    Expand review chunks using RAG retrieval for filtered Places.

    For each RAG node query, this function calls `query_reviews_filtered`
    to retrieve top_k similar review chunks **only for valid Places**.
    Duplicate review chunks are skipped.

    Parameters
    ----------
    extracted_nodes : dict
        Dictionary containing RAG queries under the "RAG" key.
    result : dict
        Expanded graph nodes with Place attributes to validate place_ids.
    top_k : int, default=5
        Number of top review chunks to return per query.

    Returns
    -------
    List[dict]
        List of dictionaries containing:
        - 'review_id': review identifier
        - 'chunk_text': chunk of review text
        - 'place_id': associated place_id
        - 'place_name': associated place name
    """
    # Return empty if no RAG queries or Place nodes
    if "RAG" not in extracted_nodes or "Place" not in result:
        return []

    # Extract valid place_ids from expanded Place nodes
    valid_place_ids = {
        str(node.get("attributes", {}).get("place_id")).strip()
        for node in result["Place"]
        if node.get("attributes", {}).get("place_id")
    }

    rag_reviews = []
    seen_reviews = set()

    # Iterate over RAG query nodes
    for rag_node in extracted_nodes["RAG"]:
        query_text = rag_node.get("text")
        if not query_text:
            continue

        # Query filtered review chunks
        query_results = query_reviews_filtered(
            query_text=query_text,
            valid_place_ids=valid_place_ids,
            top_k=top_k
        )

        # Process results, skip duplicates
        for res in query_results:
            meta = res.get("metadata", {}) or {}
            review_id = str(meta.get("review_id"))
            if review_id in seen_reviews:
                continue

            rag_reviews.append({
                "review_id": review_id,
                "chunk_text": res.get("text"),
                "place_id": meta.get("place_id"),
                "place_name": meta.get("place_name")
            })
            seen_reviews.add(review_id)

    return rag_reviews

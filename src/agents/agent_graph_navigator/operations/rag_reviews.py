"""
Review RAG Retriever
====================

This module provides functions to query ChromaDB with review texts and to expand
reviews for filtered Places using a RAG (Retrieval-Augmented Generation) approach.
It includes a default HuggingFace embedding model and example usage.

Dependencies
------------
- typing (standard library)
- langchain_huggingface
- langchain_chroma
- config.paths

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.rag_reviews import expand_reviews_with_rag
    from some_module import hetero_data, extracted_nodes, result

    rag_reviews = expand_reviews_with_rag(
        hetero_data=hetero_data,
        extracted_nodes=extracted_nodes,
        result=result,
        top_k=5
    )
"""

from typing import List, Dict, Any
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from config.paths import VECTOR_DB_GRAPH_NAVIGATOR

def query_reviews(
    review_text: str,
    top_k: int = None,
    collection_name: str = "review_embeddings",
    persist_dir: str = VECTOR_DB_GRAPH_NAVIGATOR,
    embedding_model_name: str = "neuralmind/bert-base-portuguese-cased",
    similarity_threshold: float = 0.7
) -> List[Dict[str, Any]]:
    """
    Query ChromaDB with a review text and return the top similar chunks.
    Sets up the retriever internally, including default TOP_K and similarity threshold.

    Parameters
    ----------
    review_text : str
        Text of the review to query.
    top_k : int, optional
        Number of top results to return (defaults to 5).
    collection_name : str, optional
        Name of the Chroma collection.
    persist_dir : str, optional
        Directory where the Chroma collection is persisted.
    embedding_model_name : str, optional
        Name of the HuggingFace embedding model to use.
    similarity_threshold : float, optional
        Minimum similarity threshold for results (currently placeholder, can be used later).

    Returns
    -------
    List[Dict[str, Any]]
        List of dicts with 'text' and 'metadata'.
    """
    if top_k is None:
        top_k = 5

    # Setup embedding and Chroma collection
    embedding_fn = HuggingFaceEmbeddings(model_name=embedding_model_name)
    collection = Chroma(
        collection_name=collection_name,
        embedding_function=embedding_fn,
        persist_directory=persist_dir
    )

    # Perform similarity search
    results = collection.similarity_search(review_text, k=top_k)

    # Format results
    matches = [
        {"text": doc.page_content, "metadata": doc.metadata}
        for doc in results
    ]

    return matches

def expand_reviews_with_rag(
    hetero_data, 
    extracted_nodes, 
    result, 
    top_k: int = None
) -> List[Dict[str, str]]:
    """
    Expands reviews using RAG retrieval directly from ChromaDB metadata.
    Returns review chunks with review_id, chunk_text, place_id and place_name.

    Parameters
    ----------
    hetero_data : HeteroData
        The heterogeneous graph data (not used here, kept for interface consistency).
    extracted_nodes : dict
        Dictionary of extracted nodes including 'RAG' nodes with query texts.
    result : dict
        Expanded nodes dict (not used here).
    top_k : int, optional
        Number of top retrieved reviews per query (defaults to query_reviews default).

    Returns
    -------
    rag_reviews : list of dict
        [
            {"review_id": ..., "chunk_text": ..., "place_id": ..., "place_name": ...},
            ...
        ]
    """
    if "RAG" not in extracted_nodes:
        return []

    rag_reviews = []
    seen_reviews = set()

    for rag_node in extracted_nodes["RAG"]:
        query_text = rag_node.get("text")
        if not query_text:
            continue

        # Query ChromaDB
        query_results = query_reviews(review_text=query_text, top_k=top_k)

        for res in query_results:
            metadata = res.get("metadata", {}) or {}
            review_id = str(metadata.get("review_id"))
            place_id = metadata.get("place_id")
            place_name = metadata.get("place_name")

            # Skip duplicates
            if not review_id or review_id in seen_reviews:
                continue

            rag_reviews.append({
                "review_id": review_id,
                "chunk_text": res.get("text"),
                "place_id": place_id,
                "place_name": place_name
            })
            seen_reviews.add(review_id)

    return rag_reviews
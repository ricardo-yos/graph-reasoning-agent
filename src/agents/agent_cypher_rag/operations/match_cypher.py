"""
Chroma Cypher Query Retriever
=============================

This module provides functions to retrieve the most relevant Neo4j Cypher
queries from a Chroma vector database based on a user's natural language
question. Optionally, queries can be filtered by an intent.

Dependencies
------------
- typing (standard library)
- langchain_huggingface
- langchain_chroma
- config.paths (providing VECTOR_DB_DIR)

Usage
-----
Example:

    from agents.agent_cypher_rag.operations.match_cypher import cypher_matching

    user_question = "Quais são os dados socioeconômicos do bairro Vila Bastos?"
    intent_filter = "node_info"
    matched_queries = cypher_matching(user_question, intent=intent_filter, top_k=2)
"""

from typing import List
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from config.paths import VECTOR_DB_DIR

def chroma_retriever(query_text: str, intent: str = None, top_k: int = 2) -> List[str]:
    """
    Retrieves the most relevant Cypher queries from Chroma vector DB,
    optionally filtered by intent by including the intent in the query text.

    Parameters
    ----------
    query_text : str
        User's natural language query.
    intent : str, optional
        If provided, the intent is concatenated to the query to prioritize relevant Cypher queries.
    top_k : int, optional
        Number of top results to return. Default is 2.

    Returns
    -------
    List[str]
        List of matched Cypher queries.
    """
    # Initialize embeddings
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/distiluse-base-multilingual-cased-v2"
    )

    # Connect to Chroma collection
    collection = Chroma(
        collection_name="rag_cypher",
        embedding_function=embeddings,
        persist_directory=VECTOR_DB_DIR,
    )

    # If intent is provided, append it to the query text
    query_for_embedding = f"{query_text} [INTENT: {intent}]" if intent else query_text

    # Perform similarity search
    results = collection.similarity_search(query_for_embedding, k=top_k)

    # Extract Cypher queries from results
    cypher_queries = []
    for doc in results:
        try:
            # Try to get metadata if exists
            cypher_queries.append(doc.metadata.get("cypher", doc.page_content))
        except AttributeError:
            # Fallback if doc is a string
            cypher_queries.append(str(doc))

    return cypher_queries

def cypher_matching(user_question: str, intent: str = None, top_k: int = 2) -> List[str]:
    """
    Retrieves relevant Cypher queries for a user's question, optionally filtered by intent.

    Parameters
    ----------
    user_question : str
        User's natural language question.
    intent : str, optional
        Only return Cypher queries that match this intent.
    top_k : int, optional
        Number of top results to return. Default is 2.

    Returns
    -------
    List[str]
        List of matched Cypher queries, filtered by intent if provided.
    """
    try:
        matched_cyphers = chroma_retriever(user_question, intent=intent, top_k=top_k)
        return matched_cyphers
    except Exception as e:
        print(f"Error retrieving Cypher queries: {str(e)}")
        return []

# --------------------------------------------------------
# Entry point: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Example question about neighborhood info
    question = "Quais são os dados socioeconômicos do bairro Vila Bastos?"
    intent_filter = "node_info"

    # Retrieve the top 2 matched Cypher queries from Chroma
    matched_queries = cypher_matching(question, intent=intent_filter, top_k=2)

    # Print results for inspection
    print(f"Question: {question}")
    print(f"Matched Cypher queries: {matched_queries}")

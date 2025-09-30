"""
Cypher Graph Answer Generator
==============================

This module provides a function `generate_answer_from_graph` to generate a concise
and informative answer based on extracted graph nodes and RAG-retrieved review chunks
using an LLM. It includes a test block to quickly check the function using the LLMManager.

Dependencies
------------
- langchain.schema
- typing
- llm.llm_manager (for testing only)

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.graph_answer import generate_answer_from_graph
    from llm.llm_manager import LLMManager

    llm = LLMManager()
    question = "Quais pet shops na Vila Assunção oferecem banho e tosa para cães?"
    extracted_nodes = {
        "Place": [{"attributes": {"name": "PetShop Feliz"}}],
        "Neighborhood": [{"attributes": {"name": "Vila Assunção"}}],
        "Road": [],
        "RAG": []
    }

    rag_reviews = [
        {"review_id": "review_001", "chunk_text": "Ótimo atendimento e banho completo.", 
         "place_id": "place_123", "place_name": "PetShop Feliz"}
    ]
    
    answer = generate_answer_from_graph(extracted_nodes, rag_reviews, question, llm)
"""

from langchain.schema import HumanMessage
from typing import Dict, List, Any

def generate_answer_from_graph(
    extracted_nodes: Dict[str, List[Dict[str, Any]]],
    rag_reviews: List[Dict[str, Any]],
    question: str,
    llm
) -> str:
    """
    Generates a response using the LLM based on extracted nodes and RAG-retrieved review chunks.
    Only includes Places that have relevant RAG reviews.

    Parameters
    ----------
    extracted_nodes : dict
        Dictionary containing nodes like 'Neighborhood' and 'Place' with their attributes.
    rag_reviews : list
        List of dictionaries with RAG-retrieved text chunks, including 'place_id' and 'place_name'.
    question : str
        Original user question.
    llm : object
        LLM object with a 'chat' method (like LangChain LLM wrapper).

    Returns
    -------
    str
        Generated answer from the LLM.
    """
    context_parts = []

    # Determine which Places have relevant reviews
    relevant_place_ids = {r["place_id"] for r in rag_reviews if r.get("place_id")}

    # Add other nodes (except Place)
    for node_type, nodes in extracted_nodes.items():
        if node_type == "Place":
            continue  # Place nodes handled separately
        for node in nodes:
            attributes = node.get("attributes", {})
            attr_text = ", ".join(f"{k}: {v}" for k, v in attributes.items())
            if attr_text:
                context_parts.append(f"{node_type}: {attr_text}")

    # Add only Places with relevant RAG reviews
    for node in extracted_nodes.get("Place", []):
        place_id = node.get("attributes", {}).get("place_id")
        if place_id in relevant_place_ids:
            attributes = node.get("attributes", {})
            attr_text = ", ".join(f"{k}: {v}" for k, v in attributes.items())
            context_parts.append(f"Place: {attr_text}")

    # Add RAG review chunks
    for review in rag_reviews:
        place_name = review.get("place_name") or "Unknown Place"
        chunk_text = review.get("chunk_text", "")
        context_parts.append(f"Review from {place_name}: {chunk_text}")

    # Combine all context
    context_str = "\n".join(context_parts)

    # Build prompt
    prompt = f"""
    You are an AI assistant for pet store reviews in Santo André, Brazil.

    User question: {question}

    Context from graph and retrieved reviews:
    {context_str}

    Instructions:
    - Only mention Places that appear in the provided context (including RAG reviews).
    - Do NOT invent or include any other places.
    - Provide a concise and informative answer based solely on the context.

    Answer the question accordingly.
    """

    # Call LLM
    message = HumanMessage(content=prompt)
    response = llm.chat([message])

    return response.content.strip()

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Example question
    question = "Quais pet shops na Vila Assunção oferecem banho e tosa para cães?"

    # Example extracted nodes including place_id for consistency
    extracted_nodes = {
        "Place": [
            {"attributes": {"name": "PetShop Feliz", "place_id": "place_123"}}
        ],
        "Neighborhood": [
            {"attributes": {"name": "Vila Assunção"}}
        ],
        "Road": [],
        "RAG": []
    }

    # Example RAG review chunks including place_id
    rag_reviews = [
        {"review_id": "review_001", "chunk_text": "Ótimo atendimento e banho completo.", 
         "place_id": "place_123", "place_name": "PetShop Feliz"}
    ]

    # Generate answer
    answer = generate_answer_from_graph(extracted_nodes, rag_reviews, question, llm)

    # Print result
    print("[Generated Answer]")
    print(answer)
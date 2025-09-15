"""
Cypher Answer Generator
=======================

This module provides a function `generate_cypher_answer` to generate a clear
and concise answer in Brazilian Portuguese based on a corrected user question
and the results of a Cypher query using an LLM. It includes a test block to
quickly check the function using the LLMManager.

Dependencies
------------
- json (standard library)
- typing (standard library)
- langchain.schema
- llm.llm_manager (for testing only)

Usage
-----
Example:

    from agents.agent_cypher_rag.operations.cypher_answer_generation import generate_cypher_answer
    from llm.llm_manager import LLMManager

    llm = LLMManager()
    user_question_corrected = "Quais petshops existem no bairro Campestre?"
    query_results = [
        {"name": "PetShop Feliz", "rating": 4.5, "address": "Rua A, 123"},
        {"name": "Bicho Bem", "rating": 4.0, "address": "Rua B, 45"},
    ]
    answer = generate_cypher_answer(
        user_question_corrected=user_question_corrected,
        query_results=query_results,
        llm=llm
    )
"""

import json
from typing import Any, List
from langchain.schema import SystemMessage, HumanMessage

def generate_cypher_answer(
    user_question_corrected: str,
    query_results: List[dict],
    llm: Any
) -> str:
    """
    Generates a clear answer in Brazilian Portuguese based on the user's corrected question
    and the results of a Cypher query, using LangChain message schema.

    Parameters
    ----------
    user_question_corrected : str
        The user question after fuzzy corrections.
    query_results : List[dict]
        The results returned by the Cypher query.
    llm : Any
        An LLM instance with a `.chat(messages, **kwargs)` method.

    Returns
    -------
    str
        The generated answer in Brazilian Portuguese.
    """
    # Convert query results to JSON format for prompt
    results_json = json.dumps(query_results, ensure_ascii=False)

    # Construct the prompt for the LLM
    prompt = f"""
    Given the user question:
    "{user_question_corrected}"

    And the following Cypher query result data in JSON format:
    {results_json}

    Generate a clear and concise answer in Brazilian Portuguese that responds directly to the user's question.
    Do NOT add any extra information, assumptions, or recommendations.
    """

    # Prepare LangChain messages
    messages = [
        SystemMessage(content="You are an assistant that answers questions about petshops based on Neo4j query results."),
        HumanMessage(content=prompt)
    ]

    # Query the LLM and return the generated answer
    response = llm.chat(messages, max_tokens=300, temperature=0.7)
    return response.content.strip()

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Example corrected user question
    user_question_corrected = "Quais petshops existem no bairro Campestre?"

    # Example Cypher query results (mock data)
    query_results = [
        {"name": "PetShop Feliz", "rating": 4.5, "address": "Rua A, 123"},
        {"name": "Bicho Bem", "rating": 4.0, "address": "Rua B, 45"},
    ]

    # Generate answer based on the Cypher query results
    answer = generate_cypher_answer(
        user_question_corrected=user_question_corrected,
        query_results=query_results,
        llm=llm
    )

    print("User question:", user_question_corrected)
    print("Generated answer:\n", answer)
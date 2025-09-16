"""
Neo4j Cypher Query Generator
============================

This module provides a function `generate_cypher_query` that generates a precise
Neo4j Cypher query given a user's natural language question, a set of related
Cypher queries for context, and a description of the graph schema.

Dependencies
------------
- typing (standard library)
- llm.llm_manager
- config.constants (providing GRAPH_INFO)

Usage
-----
Example:

    from agents.agent_cypher_rag.operations.cypher_generation import generate_cypher_query
    from llm.llm_manager import LLMManager
    from config.constants import GRAPH_INFO

    llm = LLMManager()
    user_question = "Quantos petshops existem no bairro Vila Assunção?"
    matched_cyphers = [
        "MATCH (n:Neighborhood {name: 'Centro'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) RETURN COUNT(p) AS total_petshops",
        "MATCH (n:Neighborhood {name: 'Centro'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) RETURN COUNT(p) AS total_petshops ORDER BY p.rating DESC"
    ]

    cypher_query = generate_cypher_query(llm, user_question, matched_cyphers, GRAPH_INFO)
"""

from typing import List
from llm.llm_manager import LLMManager
from config.constants import GRAPH_INFO

def generate_cypher_query(llm, user_question: str, matched_cyphers: List[str], graph_info: str) -> str:
    """
    Generates a Neo4j Cypher query given a user question and related Cypher queries.

    Parameters
    ----------
    llm : Any
        LLM instance with a `.chat(messages)` method for processing messages.
    user_question : str
        The user's natural language question.
    matched_cyphers : List[str]
        List of related Cypher queries retrieved as context.
    graph_info : str
        Description of the graph schema (node labels, relationships, properties).

    Returns
    -------
    str
        Generated Cypher query as a string.
    """
    try:
        context = "\n".join(matched_cyphers)

        prompt = f"""
        You are an expert Neo4j Cypher query generator assistant.

        Given the user question and the following related Cypher queries as context, 
        generate a single, precise Cypher query that answers the user's question.

        The generated Cypher query must strictly follow the structure, node labels, relationship types,
        and properties described in the graph schema below:

        Graph structure description:
        {graph_info}

        User question:
        {user_question}

        Context Cypher queries:
        {context}

        Generate only the Cypher query. Do not include explanations or additional text.
        """

        messages = [
            {"role": "system", "content": "You are a helpful Cypher query generator assistant."},
            {"role": "user", "content": prompt}
        ]

        response = llm.chat(messages)
        cypher_generated = response.content.strip() if hasattr(response, "content") else str(response)

        return cypher_generated

    except Exception as e:
        return ""

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Example user question
    user_question = "Quantos petshops existem no bairro Vila Assunção?"

    # Example matched Cypher queries (retriever output)
    matched_cyphers = [
        "MATCH (n:Neighborhood {name: 'Centro'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) RETURN COUNT(p) AS total_petshops",
        "MATCH (n:Neighborhood {name: 'Centro'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) RETURN COUNT(p) AS total_petshops ORDER BY p.rating DESC"
    ]

    generated_query = generate_cypher_query(llm, user_question, matched_cyphers, GRAPH_INFO)
    
    print("User question:", user_question)
    print("Generated Cypher query:\n", generated_query)

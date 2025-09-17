"""
RAG Cypher Agent Pipeline
=========================

This module provides a function `run_rag_cypher_pipeline` to process a user's
natural language question, detect intentions, generate and correct Cypher
queries, execute them on a Neo4j graph database, and return structured results.

It includes a test block to quickly run the full pipeline using a sample
question.

Dependencies
------------
- llm.llm_manager
- config.env_loader
- graph.neo4j.client
- agents.agent_orchestrator.agent_state
- agents.agent_cypher_rag.rag_cypher

Usage
-----
Example:

    from agents.agent_cypher_rag.rag_cypher_tester import run_rag_cypher_pipeline

    user_question = "Mostre os petshops do bairro Jardim e os dados socieconômicos"
    results = run_rag_cypher_pipeline(user_question)

    print("Detected intentions:", results["detected_intentions"])
    print("Final answer:", results["final_answer"])
"""

from typing import Dict, Any
from llm.llm_manager import LLMManager
from config.env_loader import load_env, get_neo4j_credentials
from graph.neo4j.client import Neo4jClient
from agents.agent_orchestrator.agent_state import MasterAgentState
from agents.agent_cypher_rag.rag_cypher import RAGCypherAgent

def run_rag_cypher_pipeline(user_question: str) -> Dict[str, Any]:
    """
    Run the full RAG Cypher Agent pipeline for a given user question.

    Parameters
    ----------
    user_question : str
        The natural language question provided by the user.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing detected intentions, matched queries, generated and corrected Cypher queries,
        query results, and final answer.
    """
    # Load environment variables
    load_env()

    # Initialize the LLM
    llm = LLMManager()

    # Initialize Neo4j client with credentials from environment
    uri, user, password = get_neo4j_credentials()
    neo4j_client = Neo4jClient(uri, user, password)

    # Initialize the agent
    agent = RAGCypherAgent(llm=llm, neo4j_client=neo4j_client)

    # Initialize state with the user question
    state = MasterAgentState(user_question=user_question)

    # Run the full RAGCypherAgent pipeline
    state = agent.detect_intention(state)
    state = agent.match_cypher(state)
    state = agent.generate_cypher(state)
    state = agent.correct_cypher(state)
    state = agent.run_cypher_query(state)
    state = agent.generate_cypher_answer(state)

    # Return a structured dictionary with all results
    return {
        "detected_intentions": state.cypher_state.intent_detected,
        "matched_cyphers": getattr(state.cypher_state, "matched_cyphers", []),
        "generated_cypher": getattr(state.cypher_state, "cypher_generated", ""),
        "corrected_cypher": getattr(state.cypher_state, "cypher_corrected", ""),
        "query_results": getattr(state.cypher_state, "query_results", []),
        "final_answer": state.final_response
    }

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    # User question
    user_question = "Mostre os petshops do bairro Jardim e os dados socieconômicos"
    
    # Run RAG Cypher pipeline
    results = run_rag_cypher_pipeline(user_question)

    # Print pipeline outputs
    print("Detected intentions:", results["detected_intentions"])
    print("Matched Cypher queries:", results["matched_cyphers"])
    print("Generated Cypher query:", results["generated_cypher"])
    print("Corrected Cypher query:", results["corrected_cypher"])
    print("Query results:", results["query_results"])
    print("Final answer:", results["final_answer"])
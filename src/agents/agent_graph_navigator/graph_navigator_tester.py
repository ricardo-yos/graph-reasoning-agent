"""
Graph Navigator Pipeline Runner
===============================

This module demonstrates the full workflow of the GraphNavigatorAgent:
1. Extracts relevant graph nodes from a user-provided natural language question.
2. Expands nodes by applying graph relations.
3. Retrieves relevant review chunks using RAG.
4. Generates a concise, context-aware answer using a language model (LLM).

Provides a structured example of integrating node extraction, graph reasoning, 
and RAG-based retrieval into a single pipeline.

Dependencies
------------
- agents.agent_graph_navigator.graph_navigator
- agents.agent_orchestrator.agent_state
- config.env_loader
- llm.llm_manager

Usage
-----
Example:

    from agents.agent_graph_navigator.graph_navigator_tester import run_graph_navigator_pipeline
    from agents.agent_graph_navigator.graph_navigator import GraphNavigatorAgent
    from llm.llm_manager import LLMManager

    llm_manager = LLMManager()
    agent = GraphNavigatorAgent(llm=llm_manager)
    question = "Quais locais na Vila Assunção oferecem acessórios para pets?"
    run_graph_navigator_pipeline(question, agent)
"""

from agents.agent_graph_navigator.graph_navigator import GraphNavigatorAgent
from agents.agent_orchestrator.agent_state import MasterAgentState
from config.env_loader import load_env
from llm.llm_manager import LLMManager  # Language model manager

def run_graph_navigator_pipeline(question: str, agent: GraphNavigatorAgent) -> None:
    """
    Runs the full GraphNavigatorAgent pipeline for a single question:
    1. Extract relevant nodes from the graph.
    2. Expand nodes using relations.
    3. Retrieve review chunks with RAG.
    4. Generate an LLM-based answer.

    Parameters
    ----------
    question : str
        User's natural language question.
    agent : GraphNavigatorAgent
        Initialized GraphNavigatorAgent instance with an LLM.
    """
    # Initialize the state with the user's question
    state = MasterAgentState(user_question=question)

    # 1 - Extract relevant nodes
    state = agent.extract_relevant_nodes(state)
    print("\n[1 - Extracted Nodes]")
    print(state.graph_state.extracted_nodes)

    # 2 - Define possible relationships between node types
    relation_keys = {
        "Neighborhood": {"Place": "CONTAINS", "Road": "CONTAINS"},
        "Road": {"Place": "CONTAINS"},
        "Intersection": {"Intersection": "ROAD"},
        "Place": {"Intersection": "NEAR", "Review": "HAS_REVIEW"}
    }

    # 3 - Expand nodes with relations
    state = agent.expand_all_nodes(state, relation_keys)
    print("\n[2 - Expanded Nodes]")
    print(state.graph_state.expanded_nodes)

    # 4 - Print RAG reviews
    rag_reviews = getattr(state.graph_state, "RAG_reviews", [])
    print("\n[3 - RAG Reviews]")

    if rag_reviews:
        for review in rag_reviews:
            print(review)
    else:
        print("No RAG reviews retrieved for this question.")

    # 5 - Generate answer using the LLM
    state = agent.generate_graph_answer(state)
    print("\n[4 - LLM Answer]")
    print(state.final_response)

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Load environment variables
    load_env()

    # Initialize the LLM manager and GraphNavigatorAgent
    llm_manager = LLMManager()
    agent = GraphNavigatorAgent(llm=llm_manager)

    # Test pipeline with a single question
    question = "Quais os petshops no bairro Jardim possuem elogios no atendimento?"
    run_graph_navigator_pipeline(question, agent)
"""
Graph Navigator Agent
=====================

This module defines the `GraphNavigatorAgent` class, responsible for navigating a 
heterogeneous graph of pet stores and related entities, extracting relevant nodes 
from natural language questions, expanding them with graph relations, retrieving 
reviews via RAG, and generating final answers using a language model (LLM).

The agent performs the following steps:
1. Extract relevant nodes from the user's question using a question parser.
2. Filter nodes by attributes to match the extracted criteria.
3. Expand nodes based on graph relations to include connected entities.
4. Retrieve relevant review chunks using a RAG (Retrieval-Augmented Generation) approach.
5. Generate a concise and informative answer based on the expanded nodes and RAG reviews.

Dependencies
------------
- LLM: Language model instance (e.g., LangChain LLM wrapper) with a `chat` method.
- PyTorch Geometric HeteroData: Graph representation containing nodes and attributes.
- Graph operations modules: Node extraction, filtering, relations, RAG review retrieval, and answer generation.
- config.paths: For loading model files and resources.
"""

import os
import torch
from typing import Dict
from langchain.schema import HumanMessage
from torch_geometric.data import HeteroData
from agents.agent_orchestrator.agent_state import MasterAgentState, GraphAgentState
from agents.agent_graph_navigator.operations.question_parser import question_to_nodes
from agents.agent_graph_navigator.operations.filter_nodes import filter_nodes_by_attributes
from agents.agent_graph_navigator.operations.node_relations_filter import filter_nodes_by_relations
from agents.agent_graph_navigator.operations.rag_reviews import expand_reviews_with_rag
from agents.agent_graph_navigator.operations.graph_answer import generate_answer_from_graph
from config.paths import MODELS_DIR

class GraphNavigatorAgent:
    """
    Agent responsible for navigating a heterogeneous graph of pet stores and related nodes,
    extracting relevant nodes from natural language questions, expanding them with relations,
    retrieving review data via RAG, and generating answers with a language model.

    Attributes
    ----------
    llm : object
        Language model instance (e.g., LangChain LLM wrapper) with a `chat` method.
    """

    def __init__(self, llm=None):
        """
        Initializes the GraphNavigatorAgent.

        Parameters
        ----------
        llm : object, optional
            Language model instance with a `chat` method (default is None).
        """
        self.llm = llm

    def extract_relevant_nodes(self, state: MasterAgentState) -> MasterAgentState:
        """
        Extracts relevant nodes from a user's question using an external parser and updates the graph state.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing the user question and optional graph state.

        Returns
        -------
        MasterAgentState
            Updated state with extracted nodes stored in `state.graph_state.extracted_nodes`.
        """
        question = state.user_question
        extracted_nodes = question_to_nodes(question, self.llm)

        # Initialize GraphAgentState if not present
        if state.graph_state is None:
            state.graph_state = GraphAgentState()

        state.graph_state.extracted_nodes = extracted_nodes
        return state

    def expand_all_nodes(self, state: MasterAgentState, relation_keys: Dict) -> MasterAgentState:
        """
        Expands extracted nodes by applying attribute filters, graph relations, 
        and retrieving relevant review chunks via RAG.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing extracted nodes.
        relation_keys : dict
            Dictionary mapping relationships to apply between nodes.

        Returns
        -------
        MasterAgentState
            Updated state with expanded nodes stored in `state.graph_state.expanded_nodes` 
            and RAG reviews in `state.graph_state.rag_reviews`.
        """
        if state.graph_state is None or not state.graph_state.extracted_nodes:
            return state

        extracted_nodes = state.graph_state.extracted_nodes
        hetero_path = os.path.join(MODELS_DIR, "neo4j_heterodata.pt")
        hetero_data: HeteroData = torch.load(hetero_path, weights_only=False)

        # 1 - Filter nodes by attributes
        filtered_indices, result = filter_nodes_by_attributes(hetero_data, extracted_nodes)

        # 2 - Apply graph relations
        filtered_indices, result = filter_nodes_by_relations(
            hetero_data, relation_keys, extracted_nodes, filtered_indices, result
        )

        # 3 - Expand reviews with RAG
        rag_reviews = expand_reviews_with_rag(
            extracted_nodes,
            result,
            top_k=5  # default number of retrieved reviews per query
        )

        # Save expanded nodes and RAG reviews to state
        state.graph_state.expanded_nodes = result
        state.graph_state.rag_reviews = rag_reviews

        return state

    def generate_graph_answer(self, state: MasterAgentState) -> MasterAgentState:
        """
        Generates a textual answer using the LLM based on the expanded graph nodes
        and RAG-retrieved review chunks. Delegates the main logic to an external function.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing expanded nodes, RAG reviews, and the user question.

        Returns
        -------
        MasterAgentState
            Updated state with the generated answer stored in `state.final_response`.
        """
        if state.graph_state is None:
            state.final_response = "No graph data available."
            return state

        extracted_nodes = state.graph_state.expanded_nodes or {}
        rag_reviews = getattr(state.graph_state, "rag_reviews", [])
        question = state.user_question

        # Delegate answer generation to external function
        answer = generate_answer_from_graph(
            extracted_nodes=extracted_nodes,
            rag_reviews=rag_reviews,
            question=question,
            llm=self.llm
        )

        # Update state with the final answer
        state.final_response = answer
        return state

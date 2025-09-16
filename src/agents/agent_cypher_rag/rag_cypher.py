"""
RAG-Based Cypher Agent
======================

This module defines the `RAGCypherAgent` class, which orchestrates the process of
interpreting natural language questions about a Neo4j graph, generating and correcting
Cypher queries, executing them, and producing a final answer for the user.

The agent uses the following steps:
1. Detect user intention from the input question.
2. Match the detected intents to pre-defined Cypher query patterns.
3. Generate a Cypher query using the matched patterns.
4. Apply fuzzy corrections to the generated Cypher query and user question.
5. Execute the corrected Cypher query against a Neo4j database.
6. Generate a clear answer based on the query results and corrected question.

Dependencies
------------
- LLMManager: For generating Cypher queries and answers.
- Neo4jClient: For executing Cypher queries.
- Various internal modules for intent detection, query matching, generation, and correction.
"""

from agents.agent_orchestrator.agent_state import MasterAgentState, CypherAgentState
from agents.agent_cypher_rag.operations.intent_detection import detect_intention
from agents.agent_cypher_rag.operations.match_cypher import cypher_matching
from agents.agent_cypher_rag.operations.cypher_generation import generate_cypher_query
from agents.agent_cypher_rag.operations.cypher_correction import correct_cypher_query
from agents.agent_cypher_rag.operations.cypher_answer_generation import generate_cypher_answer
from config.paths import VECTOR_DB_CYPHER_RAG
from config.constants import GRAPH_INFO

class RAGCypherAgent:
    def __init__(self, llm=None, neo4j_client=None):
        """
        Initializes the RAGCypherAgent.

        Parameters
        ----------
        llm : Any, optional
            LLM instance for generating answers and Cypher queries.
        neo4j_client : Any, optional
            Client instance for running Cypher queries in Neo4j.
        """
        self.llm = llm
        self.neo4j_client = neo4j_client

    def detect_intention(self, state: MasterAgentState) -> MasterAgentState:
        """
        Detects the user's question intentions and updates cypher_state.
        In case of failure, sets an empty list of intentions.

        Parameters
        ----------
        state : MasterAgentState
            Current agent state containing the user question.

        Returns
        -------
        MasterAgentState
            Updated state with detected intents stored in cypher_state (empty list if error occurs).
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        # Safely get the user question
        user_question = getattr(state, "user_question", "")

        try:
            cypher_state.intent_detected = detect_intention(self.llm, user_question)
        except Exception as e:
            cypher_state.intent_detected = []
            print(f"Error detecting intent: {e}")

        # Update state with detected intentions
        state.cypher_state = cypher_state

        return state
    
    def match_cypher(self, state: MasterAgentState) -> MasterAgentState:
        """
        Matches the most relevant Cypher queries for the user's question,
        using all detected intents and updating the state.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing user_question and intent_detected.

        Returns
        -------
        MasterAgentState
            Updated state with matched_cyphers.
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        try:
            user_question = state.user_question
            # Get detected intents from cypher_state
            intents = getattr(cypher_state, "intent_detected", [])

            matched_cyphers = []

            # Loop through all detected intents and fetch top-k matching Cypher queries
            for intent in intents:
                cyphers_for_intent = cypher_matching(
                    user_question=user_question,
                    intent=intent,
                    top_k=2  # top_k per intent
                )
                matched_cyphers.extend(cyphers_for_intent)

            # Remove duplicates while preserving order
            matched_cyphers = list(dict.fromkeys(matched_cyphers))

            # Store matched Cypher queries in cypher_state
            cypher_state.matched_cyphers = matched_cyphers

        except Exception as e:
            cypher_state.matched_cyphers = []
            print(f"Error matching Cypher: {e}")

        # Update state with matched Cypher queries
        state.cypher_state = cypher_state

        return state

    def generate_cypher(self, state: MasterAgentState) -> MasterAgentState:
        """
        Generates a Neo4j Cypher query for the user's question using matched Cypher queries
        from the state and updates the state with the generated query.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing user_question and cypher_state.

        Returns
        -------
        MasterAgentState
            Updated state with the generated Cypher query.
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        # Safely get the user question
        user_question = getattr(state, "user_question", "")

        try:
            # Get matched Cypher queries from the state
            matched_cyphers = getattr(cypher_state, "matched_cyphers", [])

            # Call the external function to generate the Cypher query
            cypher_generated = generate_cypher_query(
                llm=self.llm,
                user_question=user_question,
                matched_cyphers=matched_cyphers,
                graph_info=GRAPH_INFO
            )

            cypher_state.cypher_generated = cypher_generated

        except Exception:
            cypher_state.cypher_generated = ""

        # Update state with generated Cypher query
        state.cypher_state = cypher_state

        return state

    def correct_cypher(self, state: MasterAgentState) -> MasterAgentState:
        """
        Applies fuzzy correction to the generated Cypher query and updates the user question.
        Uses the external `correct_cypher_query` function. In case of error, sets defaults.

        Parameters
        ----------
        state : MasterAgentState
            The current state containing `cypher_generated` and `user_question`.

        Returns
        -------
        MasterAgentState
            Updated state with corrected Cypher query and user question.
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        cypher_generated = getattr(cypher_state, "cypher_generated", "")
        user_question = state.user_question

        try:
            # Attempt to correct both the Cypher query and the user question
            cypher_corrected, corrections, user_question_corrected = correct_cypher_query(
                cypher_generated=cypher_generated,
                user_question=user_question
            )
        except Exception as e:
            print(f"Error correcting Cypher query: {e}")
            cypher_corrected = cypher_generated
            corrections = {}
            user_question_corrected = user_question

        # Update state with corrections
        cypher_state.cypher_corrected = cypher_corrected
        cypher_state.cypher_corrections = corrections
        state.user_question_corrected = user_question_corrected
        state.cypher_state = cypher_state

        return state

    def run_cypher_query(self, state: MasterAgentState) -> MasterAgentState:
        """
        Executes the corrected Cypher query from the state using the Neo4j client
        and updates the state with the query results. In case of error, sets
        query_results to an empty list.

        Parameters
        ----------
        state : MasterAgentState
            Current state containing the Cypher query to execute.

        Returns
        -------
        MasterAgentState
            Updated state with query_results in cypher_state.
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        # Get the corrected Cypher query, or empty string if not present
        cypher_to_run = getattr(cypher_state, "cypher_corrected", "")

        try:
            # Execute the query if available
            if cypher_to_run:
                query_results = self.neo4j_client.run_query(cypher_to_run)
            else:
                query_results = []
        except Exception as e:
            print(f"Error executing Cypher query: {e}")
            query_results = []

        # Update state with query results
        cypher_state.query_results = query_results
        state.cypher_state = cypher_state

        return state

    def generate_cypher_answer(self, state: MasterAgentState) -> MasterAgentState:
        """
        Generates a clear answer for the user based on the corrected user question
        and the Cypher query results, updating the state. In case of error, sets
        final_response to an empty string.

        Parameters
        ----------
        state : MasterAgentState
            The current state containing the corrected user question and query results.

        Returns
        -------
        MasterAgentState
            Updated state with final_response filled.
        """
        # Ensure cypher_state exists
        cypher_state = state.cypher_state or CypherAgentState()

        # Safely get the corrected user question or fallback to the original user question
        user_question_corrected = getattr(cypher_state, "user_question_corrected", getattr(state, "user_question", ""))
        query_results = getattr(cypher_state, "query_results", [])

        try:
            # Call the external function to generate the answer
            answer = generate_cypher_answer(
                user_question_corrected=user_question_corrected,
                query_results=query_results,
                llm=self.llm
            )
        except Exception as e:
            print(f"Error generating Cypher answer: {e}")
            answer = ""

        # Update state with generated answer
        state.final_response = answer

        return state

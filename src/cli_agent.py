"""
Master Agent CLI Interface
==========================

This module provides a command-line interface (CLI) to interact with the
Master Agent, which orchestrates Cypher and Graph Navigator agents to answer
user questions based on Neo4j data and LLM reasoning.

Features
--------
- Cached Master Agent instance to avoid rebuilding on every run.
- Interactive REPL for asking questions to the agent.
- Prints only the final AI-generated answer.
- Suppresses warnings and logging messages from transformers and
  sentence-transformers.

Usage
-----
$ python -m cli_agent

Then type questions interactively or 'exit'/'quit' to stop the program.
"""

import os
import warnings
import logging
from functools import lru_cache
from config.env_loader import load_env, get_neo4j_credentials
from agents.agent_orchestrator.build_agent import build_master_agent
from agents.agent_orchestrator.agent_state import MasterAgentState
from llm.llm_manager import LLMManager
from graph.neo4j.client import Neo4jClient

# ----------------------------------
# Environment and Logging Setup
# ----------------------------------

warnings.filterwarnings("ignore")  # Suppress Python warnings

from transformers import logging as hf_logging
hf_logging.set_verbosity_error()  # Suppress Hugging Face warnings

logging.getLogger("sentence_transformers").setLevel(logging.ERROR)  # Suppress sentence-transformers messages

os.environ["TOKENIZERS_PARALLELISM"] = "false"  # Avoid tokenizer warnings

# ----------------------------------
# Cached Master Agent
# ----------------------------------

@lru_cache(maxsize=1)
def get_master_agent(llm_manager: LLMManager, neo4j_client: Neo4jClient):
    """
    Build and cache the Master Agent to avoid rebuilding it every run.

    Parameters
    ----------
    llm_manager : LLMManager
        Instance of the LLM manager.
    neo4j_client : Neo4jClient
        Instance of the Neo4j client.

    Returns
    -------
    StateGraph
        Cached Master Agent instance.
    """
    return build_master_agent(llm_manager, neo4j_client)

# ----------------------------------
# CLI Loop
# ----------------------------------

def cli_loop(agent):
    """
    Interactive REPL loop for the Master Agent.

    Parameters
    ----------
    agent : StateGraph
        The Master Agent instance to invoke on user input.

    Behavior
    --------
    - Reads user questions from input.
    - Invokes the agent and prints only the final answer.
    - Allows exiting with 'exit' or 'quit'.
    """
    print("Agent loaded successfully! Type your questions or 'exit' to quit.\n")

    while True:
        try:
            user_input = input("Your question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting. Goodbye!")
            break

        if not user_input:
            continue

        if user_input.lower() in ("exit", "quit"):
            print("Exiting. Goodbye!")
            break

        # Create initial state
        state = MasterAgentState(user_question=user_input)

        try:
            # Invoke the agent
            final_state = agent.invoke(state)
            final_answer = final_state.get("final_response", None)

            # Print only the AI-generated answer
            if final_answer:
                print(f"\n=== AI Response ===\n{final_answer}\n")
            else:
                print("\nNo AI response generated.\n")

        except Exception as e:
            print(f"\nError during processing: {e}\n")
            continue

# ----------------------------------
# Main Function
# ----------------------------------

def main():
    """
    CLI entrypoint for the Master Agent.

    Steps
    -----
    1. Loads environment variables.
    2. Initializes Neo4j client and LLM manager.
    3. Builds and caches the Master Agent.
    4. Starts the interactive CLI loop.
    """
    # Load environment variables
    load_env()
    uri, user, password = get_neo4j_credentials()

    # Initialize Neo4j client and LLM manager
    neo4j_client = Neo4jClient(uri, user, password)
    llm_manager = LLMManager()

    print("Loading AI agent, please wait...\n")
    agent = get_master_agent(llm_manager, neo4j_client)

    # Start CLI loop
    cli_loop(agent)

# ----------------------------------
# Entrypoint
# ----------------------------------
if __name__ == "__main__":
    main()

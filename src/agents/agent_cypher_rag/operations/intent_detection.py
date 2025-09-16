"""
Intent Detection
================

This module provides a function `detect_intention` to detect the
primary intention(s) of a user's question related to a graph database
using an LLM. It includes a test block to quickly check the function
using the LLMManager.

Dependencies
------------
- langchain
- llm.llm_manager (for testing only)

Usage
-----
Example:

    from agents.agent_cypher_rag.operations.intent_detection import detect_intention
    from llm.llm_manager import LLMManager

    llm = LLMManager()
    question = "Liste os petshops do bairro Campestre"
    intentions = detect_intention(llm, question)
"""

from typing import List
from langchain.schema import SystemMessage, HumanMessage

def detect_intention(llm, question: str) -> List[str]:
    """
    Detect the main intention(s) of a user's natural language question.

    The function sends the user's question to an LLM along with a system prompt
    describing the task and the available intentions. It expects the LLM to return
    a valid Python list of intention strings.

    Parameters
    ----------
    llm : Any
        An LLM instance with a `.chat(messages)` method capable of processing messages.
    question : str
        User's natural language question.

    Returns
    -------
    List[str]
        List of detected intentions. If the LLM response cannot be parsed as a list,
        the raw response is returned wrapped in a list.

    Raises
    ------
    ValueError
        If the `llm` instance is None.
    """
    if not llm:
        raise ValueError("LLM instance is required to detect intentions.")

    # Prepare the system and human messages for the LLM
    messages = [
        SystemMessage(
            content="""
            You are an expert graph database assistant. Your task is to identify the main **intentions**
            in a user's question based on a graph schema. 
            Do not generate Cypher queries, only return the intention(s) of the question. 

            Available intentions:
            1. list_nodes - list all nodes of a certain type
            2. filter_nodes - list nodes filtered by attributes
            3. aggregate_nodes - count or compute aggregate values of nodes
            4. distinct_values - list distinct values of a node attribute
            5. related_nodes - find nodes related to another node
            6. distance – calculate the geographic distance between two nodes
            7. node_info – retrieve detailed attributes of a specific node

            Examples:

            Question: "Liste os petshops do bairro Campestre"  
            Intentions: ["list_nodes"]

            Question: "Quais petshops do bairro Jardim têm nota maior que 4?"  
            Intentions: ["filter_nodes"]

            Question: "Quantos petshops existem no bairro Centro?"  
            Intentions: ["aggregate_nodes"]

            Question: "Quais são os tipos de lugares no bairro Bela Vista?"  
            Intentions: ["distinct_values"]

            Question: "Quais clínicas veterinárias estão próximas aos petshops do bairro Jardim?"  
            Intentions: ["related_nodes"]

            Question: "Qual é a distância entre o bairro Jardim e o bairro Centro?"
            Intentions: ["distance"]
            
            Question: "Quais são os dados socioeconômicos do bairro Vila Bastos?"
            Intentions: ["node_info"]

            Return the intention(s) as a valid Python list of strings.
            """
        ),
        HumanMessage(content=f"Question: '{question}'")
    ]

    # Send the messages to the LLM and get the response
    response = llm.chat(messages)
    raw_text = response.content if hasattr(response, "content") else str(response)

    # Try to parse the LLM response as a Python list
    try:
        intentions = eval(raw_text.strip())
        if isinstance(intentions, list):
            return intentions
    except Exception:
        pass

    # Fallback: return the raw text wrapped in a list
    return [raw_text.strip()]

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Example questions to test intention detection
    questions = [
        "Liste os petshops do bairro Campestre",
        "Quais petshops do bairro Jardim têm nota maior que 4?",
        "Quantos petshops existem no bairro Centro?"
    ]

    # Loop through each question and display the detected intentions
    for q in questions:
        intentions = detect_intention(llm, q)
        print(f"Question: {q}")
        print(f"Detected intentions: {intentions}\n")
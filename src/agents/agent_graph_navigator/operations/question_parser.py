"""
Question to Nodes Converter
===========================

This module provides a function `question_to_nodes` to convert a natural language
question into graph nodes and their attributes using an LLM. Any text that does 
not correspond to a node is placed under "RAG". It also includes a simple test 
block using LLMManager.

Dependencies
------------
- json (standard library)
- langchain.schema
- llm.llm_manager (for testing only)
- config.env_loader (for testing only)

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.question_parser import question_to_nodes
    from llm.llm_manager import LLMManager

    llm = LLMManager()

    question = "Existem clínicas veterinárias na Vila Bastos que oferecem vacinação e serviços de banho para cães e gatos?"

    extracted_nodes = question_to_nodes(question, llm)
"""

import json
from langchain.schema import HumanMessage

def question_to_nodes(question: str, llm) -> dict:
    """
    Convert a natural language question into graph nodes and attributes using an LLM.
    Any part of the question that does not correspond to a recognized node is placed under "RAG".

    Parameters
    ----------
    question : str
        The user question in natural language.
    llm : Any
        LLM instance that supports the `.chat()` method with HumanMessage input.

    Returns
    -------
    dict
        Dictionary keyed by node type with a list of node attributes. Keys include:
        'Neighborhood', 'Place', 'Road', 'Intersection', 'RAG'.

    Notes
    -----
    - For Place nodes, the type must be either 'pet_store' or 'veterinary_care'.
    - Returns empty lists for node types not found in the question.
    """
    prompt = f"""
    Instructions:
    1. Extract only the relevant nodes and their attributes from the question.
    2. If a part of the question does not match any node, put it under "RAG" with key "text", only include de main theme.
    3. Return JSON only, omit empty attributes and empty lists.
    4. Note: For Place nodes, the type must be either 'pet_store' or 'veterinary_care'.

    Format example:
    {{
        "Neighborhood": [{{"name": "Jardim"}}],
        "Place": [{{"type": "pet_store"}}],
        "Road": [{{"name": "Avenida Portugal"}}],
        "Intersection": [],
        "RAG": [{{"text": "banho e tosa"}}]
    }}

    Question: "{question}"
    """

    # Wrap the prompt into a HumanMessage for LangChain
    message = HumanMessage(content=prompt)
    
    # Query the LLM
    llm_response = llm.chat([message])

    # Attempt to parse JSON from the LLM response
    try:
        extracted_nodes = json.loads(llm_response.content.strip())
    except json.JSONDecodeError:
        # Fallback: put the whole question in RAG if parsing fails
        extracted_nodes = {"RAG": [{"text": question}]}

    return extracted_nodes

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Sample question for testing
    question = "Existem clínicas veterinárias na Avenida Paulista que atendem cães e gatos?"

    # Extract nodes from the question
    extracted_nodes = question_to_nodes(question, llm)

    # Print the results
    print("[Extracted Nodes]")
    for node_type, nodes in extracted_nodes.items():
        print(f"{node_type}: {nodes}")
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
- re (standard library)
- langchain.schema
- llm.llm_manager (for testing only)

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.question_parser import question_to_nodes
    from llm.llm_manager import LLMManager

    llm = LLMManager()

    question = "Quais pet shops existem no bairro Campestre que oferecem banho e tosa?"

    extracted_nodes = question_to_nodes(question, llm)
"""

import json
import re
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
        Dictionary keyed by node type with a list of node attributes. Keys may include:
        'Neighborhood', 'Place', 'Road', 'Intersection', 'RAG'. 
        Only non-empty keys are returned.

    Notes
    -----
    - For Place nodes, the type must be either 'pet_store' or 'veterinary_care'.
    - If parsing fails, the entire question is placed under "RAG".
    """
    prompt = f"""
    Instructions:
    1. Extract only the relevant nodes and their attributes from the question.
    2. If a part of the question does not match any node, put it under "RAG" with key "text", only include the main theme.
    3. Return JSON only, omit empty attributes and empty lists.
    4. Note: For Place nodes, the type must be either 'pet_store' or 'veterinary_care'.

    Format examples:

    Example 1 (for the question: "Existem clínicas veterinárias na Vila Bastos que oferecem vacinação e serviços de banho para cães e gatos?"):
    {{
        "Neighborhood": [{{"name": "Vila Bastos"}}],
        "Place": [{{"type": "veterinary_care"}}],
        "RAG": [{{"text": "vacinação"}}, {{"text": "banho e tosa"}}, {{"text": "cães e gatos"}}]
    }}

    Example 2 (for the question: "Tem pet shops na Avenida Portugal que fazem banho para cães?"):
    {{
        "Place": [{{"type": "pet_store"}}],
        "Road": [{{"name": "Avenida Portugal"}}],
        "RAG": [{{"text": "banho para cães"}}]
    }}

    Question: "{question}"
    """

    message = HumanMessage(content=prompt)
    llm_response = llm.chat([message])

    try:
        # Extract only the JSON portion in case LLM adds extra text
        json_text = re.search(r"\{.*\}", llm_response.content, re.DOTALL).group(0)
        parsed = json.loads(json_text)

        # Validate Place types
        if "Place" in parsed:
            valid_places = {"pet_store", "veterinary_care"}
            parsed["Place"] = [
                p for p in parsed["Place"] if p.get("type") in valid_places
            ]
            # If all invalid, remove the key
            if not parsed["Place"]:
                parsed.pop("Place")

        return parsed

    except Exception:
        # Fallback: return only RAG with the full question
        return {"RAG": [{"text": question}]}

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Import LLMManager only for testing purposes
    from llm.llm_manager import LLMManager

    # Initialize the LLM instance
    llm = LLMManager()

    # Sample question for testing
    question = "Quais pet shops existem no bairro Campestre que oferecem banho e tosa?"
    
    # Extract nodes from the question
    extracted_nodes = question_to_nodes(question, llm)

    # Print the results
    print("[Extracted Nodes]")
    for node_type, nodes in extracted_nodes.items():
        print(f"{node_type}: {nodes}")

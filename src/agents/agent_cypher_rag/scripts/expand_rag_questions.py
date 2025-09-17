"""
CSV to Cypher Variation Generator
=================================

This module reads a CSV containing base questions and corresponding Cypher queries,
generates new variations using secondary Cypher commands (e.g., WHERE, ORDER BY, LIMIT),
and saves the expanded dataset into a new CSV file.

The generation leverages an LLM (via LLMManager) to produce natural language questions
that exactly match the modified Cypher queries.

Dependencies
------------
- llm.llm_manager
- config.paths
- config.constants
- os, csv, json, re

Usage
-----
Run as a script:

    python expand_rag_questions.py

Notes
-----
This script is part of the RAG Cypher Agent pipeline.  
The generated dataset is used to improve intent detection and query matching.
"""

import os
import csv
import json
import re
from typing import List, Dict
from config.paths import RAG_DATA_DIR
from config.constants import GRAPH_INFO
from llm.llm_manager import LLMManager

# ----------------------------
# Utility function: JSON extraction
# ----------------------------

def extract_json_safe(text: str) -> dict:
    """
    Safely extract the last valid JSON object from a string.

    This function handles cases where the LLM outputs explanations, Cypher code,
    or multiple JSON objects in the same text.

    Parameters
    ----------
    text : str
        Input string potentially containing one or more JSON objects.

    Returns
    -------
    dict
        Last valid JSON object found in the string, or an empty dict if none found.
    """
    try:
        # Find all JSON-like {...} blocks in the text
        matches = re.findall(r'\{.*?\}', text, flags=re.DOTALL)
        # Attempt to parse matches starting from the last
        for match in reversed(matches):
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
    except Exception:
        pass
    return {}

# ----------------------------
# Core function: Generate Cypher variation
# ----------------------------

def generate_cypher_variation(question: str, cypher: str, command: str, llm_manager: LLMManager) -> (str, str):
    """
    Generate a new question and Cypher query variation using a secondary command.

    The function prompts the LLM to modify the original Cypher query according to
    the secondary command and produce a corresponding natural language question.

    Parameters
    ----------
    question : str
        Original natural language question.
    cypher : str
        Original Cypher query corresponding to the question.
    command : str
        Secondary command to apply (e.g., "WHERE", "ORDER BY").
    llm_manager : LLMManager
        Instance of the LLM manager for generating variations.

    Returns
    -------
    new_question : str
        Generated variation of the original question.
    new_cypher : str
        Generated variation of the original Cypher query.
    """
    prompt = f"""
    You are an expert Cypher assistant.

    Here is the graph information you must use:
    {GRAPH_INFO}

    Modify the given Cypher query by applying the following secondary command: {command}.
    Generate a natural language question that corresponds to the modified query.

    STRICT RULES:
    - Keep the same nodes and relationships. Do not invent new entities.
    - Only use properties already present in the nodes.
    - Do NOT add filters (e.g., population, rating) unless they are explicitly mentioned in the question.
    - Do NOT add LIMIT, ORDER BY, COUNT, SUM, AVG, MIN, MAX, or shortestPath unless explicitly required by the secondary command or question.
    - The Cypher query must always match exactly what the natural language question is asking — no more, no less.
    - Ensure the generated question is phrased differently from any examples provided.
      You may reorder words, use synonyms, or change sentence structure.
    - Return the output strictly as a valid JSON object.
      Do not include explanations, text, or code blocks.
      The output must follow exactly this format:

    {{
      "question": "string",
      "cypher": "string"
    }}

    --- Examples ---

    Example (WHERE):
    {{
      "question": "Liste os petshops do bairro Jardim em bairros com população maior que 1000 habitantes",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) WHERE n.population > 1000 RETURN p.name"
    }}

    Example (ORDER BY):
    {{
      "question": "Liste os petshops do bairro Jardim em ordem alfabética",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN p.name ORDER BY p.name"
    }}

    Example (LIMIT):
    {{
      "question": "Mostre os 5 primeiros petshops do bairro Jardim",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN p.name LIMIT 5"
    }}

    Example (COUNT):
    {{
      "question": "Quantos petshops existem no bairro Jardim?",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN COUNT(p) AS total_petshops"
    }}

    Example (SUM):
    {{
      "question": "Qual é a soma de reviews de todos os petshops do bairro Jardim?",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN SUM(p.num_reviews) AS total_reviews"
    }}

    Example (AVG):
    {{
      "question": "Qual é a média de rating dos petshops do bairro Jardim?",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN AVG(p.rating) AS avg_rating"
    }}

    Example (MIN):
    {{
      "question": "Qual é o petshop com menor rating no bairro Jardim?",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN p.name, p.rating ORDER BY p.rating ASC LIMIT 1"
    }}

    Example (MAX):
    {{
      "question": "Qual é o petshop com maior rating no bairro Jardim?",
      "cypher": "MATCH (n:Neighborhood {{name: 'Jardim'}})-[:CONTAINS]->(p:Place {{type: 'pet_store'}}) RETURN p.name, p.rating ORDER BY p.rating DESC LIMIT 1"
    }}

    --- Task ---

    Original question: {question}
    Original Cypher: {cypher}
    Secondary command: {command}
    """

    # Get LLM response
    response = llm_manager.chat(prompt)
    response_text = response.content if hasattr(response, "content") else str(response)

    # Attempt to parse JSON directly
    try:
        result = json.loads(response_text)
    except Exception:
        result = extract_json_safe(response_text)

    # Return generated question and Cypher query
    new_question = result.get("question", question)
    new_cypher = result.get("cypher", cypher)
    return new_question, new_cypher

# ----------------------------
# Read base CSV queries
# ----------------------------

def read_base_queries(csv_path: str) -> List[Dict[str, str]]:
    """
    Read base CSV containing questions and Cypher queries.

    Parameters
    ----------
    csv_path : str
        Path to the input CSV file.

    Returns
    -------
    List[Dict[str, str]]
        List of dictionaries with keys: "question", "cypher", and optionally "intention".
    """
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)
    return rows

# ----------------------------
# Generate variations for all commands
# ----------------------------

def generate_all_variations(base_rows: List[Dict[str, str]], llm_manager: LLMManager, commands: List[str]) -> List[Dict[str, str]]:
    """
    Generate all variations for each base question using all secondary commands.

    Parameters
    ----------
    base_rows : List[Dict[str, str]]
        Base CSV rows containing questions and Cypher queries.
    llm_manager : LLMManager
        LLM manager instance used to generate variations.
    commands : List[str]
        List of secondary commands to apply (e.g., "WHERE", "ORDER BY").

    Returns
    -------
    List[Dict[str, str]]
        Expanded list including all generated variations and original rows.
    """
    output_rows = []
    for idx, row in enumerate(base_rows, 1):
        question = row["question"]
        cypher = row["cypher"]
        intention = row.get("intention", "")
        print(f"\nProcessing row {idx}: {question}")

        for command in commands:
            print(f"  → Generating variation for command '{command}'...")
            try:
                new_question, new_cypher = generate_cypher_variation(question, cypher, command, llm_manager)
                output_rows.append({
                    "question": new_question,
                    "intention": intention,
                    "cypher": new_cypher
                })
            except Exception as e:
                print(f"Failed for command '{command}': {e}")
                # If generation fails, fallback to original
                output_rows.append({
                    "question": question,
                    "intention": intention,
                    "cypher": cypher
                })

        # Include the original query as well
        output_rows.append({
            "question": question,
            "intention": intention,
            "cypher": cypher
        })
    return output_rows

# ----------------------------
# Save generated variations
# ----------------------------

def save_variations(output_rows: List[Dict[str, str]], output_path: str):
    """
    Save the generated variations to a CSV file.

    Parameters
    ----------
    output_rows : List[Dict[str, str]]
        List of all question-Cypher variations.
    output_path : str
        Path to save the output CSV file.
    """
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["question", "intention", "cypher"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"\nExpanded CSV with Cypher variations saved to {output_path}")
    print(f"Total rows in output: {len(output_rows)}")

# ----------------------------
# Main execution
# ----------------------------

if __name__ == "__main__":
    # Initialize LLMManager
    llm_manager = LLMManager()

    # Define secondary commands
    SECONDARY_COMMANDS = ["WHERE", "ORDER BY", "LIMIT", "COUNT", "SUM", "AVG", "MIN", "MAX"]

    # Define file paths
    csv_path = os.path.join(RAG_DATA_DIR, "rag_base_queries.csv")
    output_path = os.path.join(RAG_DATA_DIR, "rag_questions_cypher.csv")

    # Read base queries from CSV
    base_rows = read_base_queries(csv_path)

    # Generate variations for all commands
    output_rows = generate_all_variations(base_rows, llm_manager, SECONDARY_COMMANDS)

    # Save all generated variations to CSV
    save_variations(output_rows, output_path)
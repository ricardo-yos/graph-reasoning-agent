"""
CSV to JSON Converter for Cypher RAG Questions
==============================================

This script converts a CSV file containing Cypher RAG questions into a JSON file.

Features
--------
- Reads a CSV file with columns: 'question', 'intention', and 'cypher'.
- Converts the rows into JSON objects.
- Saves the output JSON file with UTF-8 encoding and indentation for readability.

Usage
-----
Run this script directly to convert the default file located in RAG_DATA_DIR:

    python csv_to_json.py
"""

import os
import csv
import json
from typing import List, Dict
from config.paths import RAG_DATA_DIR

def csv_to_json(csv_filepath: str, json_filepath: str) -> None:
    """
    Convert a CSV file containing Cypher RAG questions into a JSON file.

    Parameters
    ----------
    csv_filepath : str
        Path to the input CSV file containing the RAG Cypher questions.
    json_filepath : str
        Path where the output JSON file will be saved.

    Notes
    -----
    - The CSV file must have the following columns:
      'question', 'intention', 'cypher'.
    - The output JSON will be saved with UTF-8 encoding and indentation for readability.
    """
    data: List[Dict[str, str]] = []
    with open(csv_filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            data.append({
                "question": row["question"],
                "intention": row["intention"],
                "cypher": row["cypher"]
            })

    # Save the JSON file
    with open(json_filepath, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=4, ensure_ascii=False)

    print(f"JSON file successfully created at: {json_filepath}")

# ----------------------------
# Entry point
# ----------------------------

if __name__ == "__main__":
    csv_filepath: str = os.path.join(RAG_DATA_DIR, "rag_questions_cypher.csv")
    json_filepath: str = os.path.join(RAG_DATA_DIR, "rag_questions_cypher.json")
    csv_to_json(csv_filepath, json_filepath)
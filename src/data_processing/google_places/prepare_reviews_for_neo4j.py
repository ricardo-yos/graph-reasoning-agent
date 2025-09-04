"""
CSV Column Normalization for Neo4j Import
=========================================

This script processes a CSV file containing Google Places reviews by converting
all column names to snake_case. This normalization ensures consistency when
importing data into Neo4j, avoiding case-sensitivity issues and making
property names easier to reference in Cypher queries.

Main steps:
1. Read the raw CSV file with reviews.
2. Convert column names to snake_case using regular expressions.
3. Save the processed CSV for later Neo4j import.

Reason for Neo4j:
- Neo4j differentiates between upper and lower case in property names.
- Standardizing column names avoids mismatches when creating nodes and relationships.
"""

import os
import re
import pandas as pd
from config.paths import RAW_GOOGLE_PLACES_DIR, PROCESSED_GOOGLE_PLACES_DIR

def to_snake_case(name: str) -> str:
    """
    Convert a string to snake_case.
    
    Parameters
    ----------
    name : str
        Original string (column name).
        
    Returns
    -------
    str
        Converted string in snake_case.
    """
    # Replace spaces and hyphens with underscores
    name = re.sub(r"[\s\-]+", "_", name)
    # Add underscore before uppercase letters that follow lowercase or digits
    name = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    return name.lower()

def main():
    """
    Main execution function:
    1. Reads the raw reviews CSV.
    2. Converts all column names to snake_case.
    3. Saves the processed CSV for Neo4j import.
    """
    input_path = os.path.join(RAW_GOOGLE_PLACES_DIR, "reviews.csv")
    output_path = os.path.join(PROCESSED_GOOGLE_PLACES_DIR, "reviews_processed.csv")

    # Read CSV
    df = pd.read_csv(input_path, sep=';', encoding="utf-8")

    # Convert columns to snake_case
    df.columns = [to_snake_case(col) for col in df.columns]

    # Save processed CSV
    df.to_csv(output_path, sep=';', index=False)
    print(f"Converted CSV saved to: {output_path}")

if __name__ == "__main__":
    main()

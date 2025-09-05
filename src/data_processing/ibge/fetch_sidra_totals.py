"""
SIDRA Totals Extraction for Santo André Neighborhoods
=====================================================

This script processes raw SIDRA CSV files downloaded for Santo André neighborhoods.
It extracts total values from each table, handles special cases (e.g., table 3170),
and compiles the data into a structured CSV file in long format.

Steps:
1. Load neighborhood ID-to-name mapping from Excel.
2. Iterate over neighborhood folders in RAW_SANTO_ANDRE_SIDRA_DIR.
3. Extract total values from all CSV files per neighborhood.
4. Handle special tables with multiple variables.
5. Save the combined results as a CSV file in INTERIM_SANTO_ANDRE_SIDRA_DIR.

Logs are saved to 'sidra_totals.log'.
"""

import re
import csv
import pandas as pd
from pathlib import Path
from utils.logger import setup_logger
from config.paths import RAW_SANTO_ANDRE_SIDRA_DIR, INTERIM_SANTO_ANDRE_SIDRA_DIR
from config.constants import TABLE_ID_TO_NAME, TABLE_ID_TO_YEAR

# ==============================
# SETUP LOGGER
# ==============================

logger = setup_logger("sidra_totals", level="INFO", log_filename="sidra_totals.log")

# ==============================
# EXTRACT TOTALS FROM A SINGLE CSV FILE
# ==============================

def extract_all_totals_from_csv(file_path: Path, table_id: str) -> list:
    """
    Extract all 'total' values from a SIDRA CSV file.

    Parameters
    ----------
    file_path : Path
        Path to the input CSV file to be processed.
    table_id : str
        Identifier of the SIDRA table.

    Returns
    -------
    list of str
        List of extracted numeric values as strings, formatted with '.' as the decimal separator.
    """
    totals = []
    try:
        with file_path.open('r', encoding='utf-8') as f:
            reader = list(csv.reader(f))

            # Case 1: "Total" in second column, value in third column
            for row in reader:
                if len(row) >= 3 and row[1].strip().lower() == "total":
                    val_str = row[2].strip().replace(',', '.')
                    if val_str and val_str.lower() != "total":
                        totals.append(val_str)

            # Case 2: "Total" anywhere, value in same column next row
            for i, row in enumerate(reader[:-1]):
                for j, cell in enumerate(row):
                    if cell.strip().lower() == "total":
                        next_row = reader[i + 1]
                        if j < len(next_row):
                            val_str = next_row[j].strip().replace(',', '.')
                            if val_str and val_str.lower() != "total":
                                totals.append(val_str)

            # Case 3: All middle columns "total", value in last column
            for row in reader:
                if len(row) >= 4 and all(row[i].strip().lower() == "total" for i in range(1, len(row) - 1)):
                    val_str = row[-1].strip().replace(',', '.')
                    if val_str and val_str.lower() != "total":
                        totals.append(val_str)

    except Exception as e:
        logger.warning(f"Failed to process file {file_path.name}: {e}")

    return totals

# ==============================
# HANDLE SINGLE CSV EXTRACTION AND SPECIAL CASES
# ==============================

def extract_and_handle_csv(file: Path, neighborhood_id: str, neighborhood: str) -> list:
    """
    Extract totals from a single CSV file and handle special cases (e.g., table 3170).

    Parameters
    ----------
    file : Path
        CSV file path.
    neighborhood_id : str
        Neighborhood ID.
    neighborhood : str
        Neighborhood name.

    Returns
    -------
    list of dict
        Extracted records in long format.
    """
    rows = []
    match = re.search(r'\d+', file.stem)
    table_id = match.group() if match else file.stem

    totals = extract_all_totals_from_csv(file, table_id)
    if not totals:
        logger.warning(f"No totals found for table {table_id} in file {file.name}")
        return []

    # Special case: table 3170 has two variables
    if table_id == "3170":
        if len(totals) >= 1:
            rows.append({
                "Neighborhood_ID": neighborhood_id,
                "Neighborhood": neighborhood,
                "Variable": TABLE_ID_TO_NAME["3170_1"],
                "Year": TABLE_ID_TO_YEAR["3170_1"],
                "Value": totals[0]
            })
        if len(totals) >= 2:
            rows.append({
                "Neighborhood_ID": neighborhood_id,
                "Neighborhood": neighborhood,
                "Variable": TABLE_ID_TO_NAME["3170_2"],
                "Year": TABLE_ID_TO_YEAR["3170_2"],
                "Value": totals[1]
            })
        return rows

    # Default case for standard tables
    variable = TABLE_ID_TO_NAME.get(table_id)
    year = TABLE_ID_TO_YEAR.get(table_id)
    if not variable or not year:
        logger.warning(f"Unknown table ID: {table_id}. Skipping.")
        return []

    rows.append({
        "Neighborhood_ID": neighborhood_id,
        "Neighborhood": neighborhood,
        "Variable": variable,
        "Year": year,
        "Value": totals[0]  # Only the first value for standard tables
    })
    return rows

# ==============================
# PROCESS ALL CSVS IN A NEIGHBORHOOD FOLDER
# ==============================

def extract_rows_from_neighborhood(neighborhood_dir: Path, neighborhood_id: str, neighborhood: str) -> list:
    """
    Process all CSV files in a neighborhood folder and return structured rows.

    Parameters
    ----------
    neighborhood_dir : Path
        Directory containing CSV files for the neighborhood.
    neighborhood_id : str
        Neighborhood ID.
    neighborhood : str
        Neighborhood name.

    Returns
    -------
    list of dict
    """
    rows = []
    for file in sorted(neighborhood_dir.glob("*.csv")):
        rows.extend(extract_and_handle_csv(file, neighborhood_id, neighborhood))
    return rows

# ==============================
# LOAD NEIGHBORHOOD ID-TO-NAME MAPPING
# ==============================

def load_neighborhood_mapping(excel_path: Path) -> dict:
    """
    Load mapping of neighborhood ID to name from Excel file.

    Parameters
    ----------
    excel_path : Path
        Path to the Excel file containing the mapping.

    Returns
    -------
    dict
        Dictionary with keys as neighborhood IDs and values as neighborhood names.
    """
    logger.info(f"Loading neighborhood mapping from {excel_path}")

    # Load the Excel file without headers
    df = pd.read_excel(excel_path, header=None, dtype=str)

    # Define column names
    df.columns = ["neighborhood_id", "neighborhood"]

    # Strip whitespace from IDs and names
    df["neighborhood_id"] = df["neighborhood_id"].str.strip()
    df["neighborhood"] = df["neighborhood"].str.strip()

    # Remove suffix like " - Santo André (SP)" from names
    df["neighborhood"] = df["neighborhood"].str.replace(
        r"\s+-\s+Santo André\s*\(SP\)", "", regex=True
    ).str.strip()

    # Create dictionary mapping ID -> neighborhood name
    mapping = dict(zip(df["neighborhood_id"], df["neighborhood"]))

    logger.info(f"Loaded {len(mapping)} neighborhoods")
    return mapping

# ==============================
# SAVE EXTRACTED ROWS TO CSV
# ==============================

def save_rows_to_csv(rows: list, output_file: Path) -> None:
    """
    Save structured rows to a CSV file.

    Parameters
    ----------
    rows : list of dict
        List of structured rows to save.
    output_file : Path
        Path to save the CSV file.
    """
    rows.sort(key=lambda x: int(x["Neighborhood_ID"]))
    with output_file.open("w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Neighborhood_ID", "Neighborhood", "Variable", "Year", "Value"],
            delimiter=";"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.info(f"Saved results to {output_file.absolute()}")

# ==============================
# MAIN EXTRACTION FUNCTION
# ==============================

def extract_totals_matrix() -> None:
    """
    Extract totals from all SIDRA CSV files and save as a CSV matrix.

    Steps
    -----
    1. Load neighborhood mapping from Excel.
    2. Iterate over neighborhood folders.
    3. Extract totals from CSVs.
    4. Save results as CSV.
    """
    logger.info("Starting extraction of totals from SIDRA CSVs...")
    mapping_file = Path(RAW_SANTO_ANDRE_SIDRA_DIR) / "Bairro em Município - Santo André (SP).xlsx"
    neighborhood_id_to_name = load_neighborhood_mapping(mapping_file)

    output_rows = []
    for neighborhood_dir in sorted(Path(RAW_SANTO_ANDRE_SIDRA_DIR).iterdir()):
        if not neighborhood_dir.is_dir():
            continue
        neighborhood_id = neighborhood_dir.name
        neighborhood = neighborhood_id_to_name.get(neighborhood_id)
        if neighborhood is None:
            logger.warning(f"Neighborhood '{neighborhood_id}' not found in mapping. Skipping.")
            continue
        logger.info(f"Processing neighborhood: {neighborhood} (ID: {neighborhood_id})")
        rows = extract_rows_from_neighborhood(neighborhood_dir, neighborhood_id, neighborhood)
        output_rows.extend(rows)

    output_file = Path(INTERIM_SANTO_ANDRE_SIDRA_DIR) / "neighborhoods_sidra_long.csv"
    save_rows_to_csv(output_rows, output_file)

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    extract_totals_matrix()
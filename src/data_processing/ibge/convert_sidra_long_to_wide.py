"""
SIDRA Long-to-Wide Conversion for Santo André Neighborhoods
===========================================================

This script converts SIDRA neighborhood data from long format to wide format.
It drops the 'Year' column, ensures the 'Value' column is numeric, and fills
missing values with NaN. The resulting CSV is saved to the processed data folder.

Steps:
1. Load the long-format CSV from INTERIM_SANTO_ANDRE_SIDRA_DIR.
2. Convert 'Value' column to numeric and drop 'Year'.
3. Pivot the data to wide format, with variables as columns.
4. Save the wide-format CSV to PROCESSED_SANTO_ANDRE_SIDRA_DIR.
"""

import pandas as pd
from pathlib import Path
from config.paths import INTERIM_SANTO_ANDRE_SIDRA_DIR, PROCESSED_SANTO_ANDRE_SIDRA_DIR

# ==============================
# CONVERT LONG-FORMAT CSV TO WIDE-FORMAT
# ==============================

def convert_long_to_wide(input_csv: Path, output_csv: Path) -> None:
    """
    Convert a long-format CSV to wide-format, dropping the 'Year' column and filling missing values with NaN.

    Parameters
    ----------
    input_csv : Path
        Path to the input CSV file in long format.
    output_csv : Path
        Path where the wide-format CSV will be saved.

    Returns
    -------
    None
        Saves the converted CSV to the specified output path.
    """
    print(f"Starting conversion from long to wide: {input_csv}")

    # Load long-format CSV
    df = pd.read_csv(input_csv, sep=';')

    # Convert 'Value' column to numeric (float), coercing errors to NaN
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

    # Drop 'Year' column if exists
    if 'Year' in df.columns:
        df = df.drop(columns=['Year'])

    # Pivot the DataFrame from long to wide format
    df_wide = df.pivot_table(
        index=["Neighborhood_ID", "Neighborhood"],
        columns="Variable",
        values="Value"
    ).reset_index()

    # Ensure output directory exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Save the wide-format CSV
    df_wide.to_csv(output_csv, sep=';', index=False, na_rep="nan")

    print(f"Finished conversion and saved to {output_csv}")

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    input_csv_path = Path(INTERIM_SANTO_ANDRE_SIDRA_DIR) / "neighborhoods_sidra_long.csv"
    output_csv_path = Path(PROCESSED_SANTO_ANDRE_SIDRA_DIR) / "neighborhoods_sidra_wide.csv"

    convert_long_to_wide(input_csv_path, output_csv_path)
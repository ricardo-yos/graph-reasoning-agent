"""
Retry Failed Downloads Script
=============================

This script retries downloading SIDRA tables for valid neighborhood-table pairs.

Context
-------
The dictionary `VALID_COMBINATIONS` was manually corrected based on 
`failed_downloads.json`. Some downloads failed because certain table IDs 
differed from those in the automatic `TABLES` list. `VALID_COMBINATIONS` 
now contains only valid neighborhood-table pairs for retrying downloads.

Workflow
--------
1. Iterate over all valid (neighborhood, table_id) combinations.
2. Ensure the output directory exists for each neighborhood.
3. Retry the download in parallel using ThreadPoolExecutor.
4. Log successes and collect any remaining failures.

Notes
-----
- Logs are written to `logs/retry_downloads.log`.
- Remaining failed combinations are printed at the end for further review.
"""

import json
from pathlib import Path
from utils.logger import setup_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_processing.sidra_downloader import process_table_for_neighborhood
from config.paths import RAW_SANTO_ANDRE_SIDRA_DIR
from config.constants import VALID_COMBINATIONS

# ==============================
# SETUP LOGGER
# ==============================

logger = setup_logger("retry_downloads", level="DEBUG", log_filename="retry_downloads.log")

# ==============================
# FUNCTION: RETRY DOWNLOADS
# ==============================

def retry_valid_combinations() -> None:
    """
    Retry downloading SIDRA tables for valid neighborhood-table pairs.

    Step-by-step:
    1. Count total number of combinations to process.
    2. Iterate over the VALID_COMBINATIONS dictionary.
    3. For each (neighborhood, table_id):
        a. Ensure output directory exists.
        b. Submit the download task to a ThreadPoolExecutor.
    4. As each task completes:
        a. Log success message.
        b. If any exception occurs, log error and add to failure list.
    5. After all downloads, log summary of failures or confirm success.

    Notes
    -----
    - Expects `VALID_COMBINATIONS` to be a dictionary of the form:
        {
            "Neighborhood1": ["Table1", "Table2"],
            "Neighborhood2": ["Table3"],
            ...
        }
    - Logs are saved to `logs/retry_downloads.log`.

    Examples
    --------
    >>> retry_valid_combinations()
    """
    # Count total combinations
    total = sum(len(tables) for tables in VALID_COMBINATIONS.values())
    logger.info(f"Retrying {total} valid combinations...")

    # Prepare for multithreaded execution
    futures = []
    future_to_combo = {}
    failed_combinations = []

    # Submit tasks to ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        for neighborhood, table_ids in VALID_COMBINATIONS.items():
            for table_id in table_ids:
                # Ensure the neighborhood-specific directory exists
                neighborhood_dir = Path(RAW_SANTO_ANDRE_SIDRA_DIR) / neighborhood
                neighborhood_dir.mkdir(parents=True, exist_ok=True)

                logger.debug(f"Submitting task: Neighborhood='{neighborhood}', TableID='{table_id}'")

                # Submit the download task
                future = executor.submit(
                    process_table_for_neighborhood,
                    table_id,
                    neighborhood,
                    neighborhood_dir
                )
                futures.append(future)
                future_to_combo[future] = (neighborhood, table_id)

        # Process completed tasks
        for future in as_completed(futures):
            neighborhood, table_id = future_to_combo[future]
            try:
                future.result()
                logger.info(f"Success: Neighborhood='{neighborhood}', TableID='{table_id}'")
            except Exception as e:
                logger.error(f"Failed: Neighborhood='{neighborhood}', TableID='{table_id}' - {e}")
                failed_combinations.append({
                    "neighborhood": neighborhood,
                    "table_id": table_id
                })

    # Log summary of failures
    if failed_combinations:
        logger.warning(f"{len(failed_combinations)} combinations failed.")
        for fail in failed_combinations:
            logger.debug(f"Failed combination: Neighborhood='{fail['neighborhood']}', TableID='{fail['table_id']}'")
    else:
        logger.info("All downloads succeeded.")

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    retry_valid_combinations()
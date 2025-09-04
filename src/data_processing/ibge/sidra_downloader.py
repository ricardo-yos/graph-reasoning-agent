"""
SIDRA Neighborhood Data Downloader for Santo André
==================================================

This script automates downloading socio-economic indicators from IBGE's SIDRA
platform for each neighborhood in Santo André. It navigates SIDRA's web interface
using Selenium, selects desired variables, downloads CSV files, and stores
them in structured directories. Failed downloads are logged for later review.

Features:
- Multi-threaded downloads using ThreadPoolExecutor.
- Automatic creation of neighborhood directories.
- Waits for table and download modal to fully load.
- Handles variable selection for specific tables (e.g., 3170).
- Logs all progress, warnings, and errors to sidra_downloader.log.
"""

import time
import json
from pathlib import Path
from utils.logger import setup_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from config.paths import RAW_SANTO_ANDRE_SIDRA_DIR, INTERIM_SANTO_ANDRE_SIDRA_DIR
from config.constants import TABLES, NEIGHBORHOODS, DESIRED_VARIABLES

# ==============================
# SETUP LOGGER
# ==============================

logger = setup_logger("sidra_downloader", level="INFO", log_filename="sidra_downloader.log")

# ==============================
# DIRECTORY MANAGEMENT
# ==============================

def create_base_directory() -> None:
    """
    Ensure the base directory for downloads exists.
    """
    Path(RAW_SANTO_ANDRE_SIDRA_DIR).mkdir(parents=True, exist_ok=True)

# ==============================
# WEBDRIVER SETUP
# ==============================

def setup_driver(download_dir: Path) -> webdriver.Chrome:
    """
    Initialize Chrome WebDriver with custom download directory.

    Parameters
    ----------
    download_dir : Path
        Directory where files will be downloaded.

    Returns
    -------
    webdriver.Chrome
        Configured Chrome WebDriver instance.
    """
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--window-size=800,600")
    return webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=options)

# ==============================
# TABLE LOADING & NAVIGATION
# ==============================

def wait_for_table_to_load(driver: webdriver.Chrome, timeout: int = 30) -> None:
    """
    Wait until the SIDRA table page finishes loading.

    Parameters
    ----------
    driver : webdriver.Chrome
        Selenium WebDriver instance.
    timeout : int
        Maximum wait time in seconds.
    """
    WebDriverWait(driver, timeout).until(
        lambda d: "carregado" in d.find_element(By.TAG_NAME, "body").get_attribute("class")
    )

def open_table_page(driver: webdriver.Chrome, table: str, neighborhood: str) -> None:
    """
    Navigate to the specified SIDRA table for a given neighborhood.

    Parameters
    ----------
    driver : webdriver.Chrome
        Active WebDriver.
    table : str
        SIDRA table ID.
    neighborhood : str
        Neighborhood code.
    """
    url = f"https://sidra.ibge.gov.br/Tabela/{table}#/N102/{neighborhood}"
    driver.get(url)
    wait_for_table_to_load(driver)

# ==============================
# VARIABLE SELECTION (TABLE 3170)
# ==============================

def select_variables(driver: webdriver.Chrome) -> None:
    """
    Select predefined variables on SIDRA's variable panel.

    Parameters
    ----------
    driver : webdriver.Chrome
        Active WebDriver.

    Notes
    -----
    Only applicable for certain tables like 3170.
    """
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "panel-V")))
    except Exception as e:
        logger.warning(f"Variable panel not found: {e}")
        return

    for label in DESIRED_VARIABLES:
        try:
            checkbox = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    f"//span[text()='{label}']/preceding-sibling::button[contains(@class, 'sidra-toggle')]"
                ))
            )
            if checkbox.get_attribute("aria-selected") == "false":
                driver.execute_script("arguments[0].click();", checkbox)
        except Exception as e:
            logger.warning(f"Could not select variable '{label}': {e}")

# ==============================
# DOWNLOAD MODAL HANDLING
# ==============================

def open_download_modal(driver: webdriver.Chrome) -> None:
    """
    Open the download modal on the SIDRA page.

    Parameters
    ----------
    driver : webdriver.Chrome
        Active WebDriver.
    """

    # Wait until the download button is clickable and then click it via JavaScript to avoid issues
    try:
        download_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "botao-downloads"))
        )
        driver.execute_script("arguments[0].click();", download_btn)
    except Exception as e:
        logger.warning(f"Could not click download button: {e}")
        return

    try:
        WebDriverWait(driver, 10).until(
            lambda d: any(
                d.find_element(By.ID, modal).is_displayed()
                for modal in ["modal-gerar-links", "modal-downloads", "modal-download-quadro"]
            )
        )
        time.sleep(1)
    except Exception as e:
        logger.warning(f"Download modal did not open properly: {e}")

def select_format_and_click_download(driver: webdriver.Chrome) -> None:
    """
    Choose CSV (US) format in the download modal and initiate download.

    Parameters
    ----------
    driver : webdriver.Chrome
        Active WebDriver.
    """

    # Define modal IDs and their select/button CSS selectors
    modals = {
        "modal-gerar-links": {"select": "select.select-formato-arquivo", "btn_download": "button#btn-confirmar-download"},
        "modal-downloads": {"select": "select.select-formato-arquivo", "btn_download": "a#opcao-downloads"},
        "modal-download-quadro": {"select": "select.formato-quadro", "btn_download": "a#download-quadro"}
    }

    active_modal = next((m for m in modals if driver.find_element(By.ID, m).is_displayed()), None)
    if not active_modal:
        logger.warning("No visible download modal found.")
        return

    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{active_modal} {modals[active_modal]['select']}"))
        )
        Select(select_element).select_by_value("us.csv")
        time.sleep(1)

        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{active_modal} {modals[active_modal]['btn_download']}"))
        )
        driver.execute_script("arguments[0].click();", download_button)
        time.sleep(5)
    except Exception as e:
        logger.warning(f"Error during format selection or download click: {e}")

# ==============================
# WAIT FOR DOWNLOADS
# ==============================

def wait_for_all_downloads(download_dir: Path, expected_files: int, timeout: int = 120) -> list[Path]:
    """
    Wait until all expected .csv files appear in the download directory.

    Parameters
    ----------
    download_dir : Path
        Download directory path.
    expected_files : int
        Number of expected CSV files.
    timeout : int
        Max wait time in seconds.

    Returns
    -------
    list[Path]
        List of downloaded files.

    Raises
    ------
    TimeoutError
        If not all files are downloaded in time.
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        csv_files = list(download_dir.glob("*.csv"))
        temp_files = list(download_dir.glob("*.crdownload"))
        if len(csv_files) >= expected_files and not temp_files:
            return csv_files
        time.sleep(1)

    logger.error(f"Only {len(csv_files)} of {expected_files} expected files downloaded.")
    raise TimeoutError("Download did not complete in time.")

# ==============================
# PROCESS SINGLE TABLE
# ==============================

def process_table_for_neighborhood(table: str, neighborhood: str, download_dir: Path) -> None:
    """
    Download a specific SIDRA table for a given neighborhood.

    Parameters
    ----------
    table : str
        SIDRA table ID.
    neighborhood : str
        IBGE neighborhood code.
    download_dir : Path
        Path to save the downloaded CSV file.
    """
    driver = setup_driver(download_dir)
    try:
        logger.info(f"[{neighborhood}] Processing table {table}")
        open_table_page(driver, table, neighborhood)
        if table == "3170":
            logger.info(f"[{neighborhood}] Selecting variables for table {table}")
            select_variables(driver)
        open_download_modal(driver)
        select_format_and_click_download(driver)
        logger.info(f"[{neighborhood}] Table {table} processed successfully")
    except Exception as e:
        logger.error(f"[{neighborhood}] Error in table {table}: {e}", exc_info=True)
    finally:
        logger.debug(f"[{neighborhood}] Closing driver for table {table}")
        driver.quit()

# ==============================
# MAIN DOWNLOADER EXECUTION
# ==============================

def run_downloader():
    """
    Run the downloader process to fetch all SIDRA tables for each neighborhood in parallel.

    This function:

    Step-by-step:
    1. Initializes logger and prints a starting message.
    2. Creates the root directory where all SIDRA files will be saved.
    3. Iterates through each neighborhood in the NEIGHBORHOODS list.
    4. For each neighborhood:
        a. Creates a subdirectory for that neighborhood.
        b. Uses ThreadPoolExecutor to download all tables in parallel.
        c. Handles any download exceptions.
        d. Verifies that the expected number of files have been downloaded.
    5. Collects failed downloads and saves them to a JSON file.
    6. Logs final completion message.

    Notes
    -----
    All logs (errors, progress, warnings) are saved to sidra_downloader.log.
    """
    logger.info("Starting SIDRA data download process...")

    create_base_directory()
    failed_downloads = []

    for neighborhood in NEIGHBORHOODS:
        # Step 1: Create directory for the neighborhood
        neighborhood_dir = Path(RAW_SANTO_ANDRE_SIDRA_DIR) / neighborhood
        neighborhood_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading data for neighborhood {neighborhood} into {neighborhood_dir}")

        # Step 2: Download all SIDRA tables in parallel using threads
        with ThreadPoolExecutor(max_workers=len(TABLES)) as executor:
            futures = [
                executor.submit(process_table_for_neighborhood, table, neighborhood, neighborhood_dir)
                for table in TABLES
            ]

            for table, future in zip(TABLES, futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[{neighborhood}] Error downloading table {table}: {e}", exc_info=True)
                    failed_downloads.append({
                        "neighborhood": neighborhood,
                        "table": table,
                        "error": str(e)
                    })

        # Step 3: Wait until all downloads complete
        try:
            wait_for_all_downloads(neighborhood_dir, expected_files=len(TABLES), timeout=60)
        except TimeoutError as e:
            logger.error(f"[{neighborhood}] Timeout waiting for downloads: {e}")
            for table in TABLES:
                failed_downloads.append({
                    "neighborhood": neighborhood,
                    "table": table,
                    "error": "timeout"
                })

    # Step 4: Save failed downloads
    if failed_downloads:
        failed_path = Path(INTERIM_SANTO_ANDRE_SIDRA_DIR) / "failed_downloads.json"
        with failed_path.open("w", encoding="utf-8") as f:
            json.dump(failed_downloads, f, ensure_ascii=False, indent=2)
        logger.warning(f"{len(failed_downloads)} downloads failed. Details saved to {failed_path}")

    logger.info("All downloads completed for all neighborhoods!")

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    run_downloader()

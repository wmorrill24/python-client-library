import os
import yaml
import requests
import logging
from .exceptions import APIError, FileConfigurationError
import pandas as pd
# from rich.console import Console

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
API_BASE_URL = os.getenv("INGEST_API_URL", "http://localhost:8001")


def generate_metadata_template(filepath: str = ".", overwrite: bool = False):
    """
    Generates a blank metadata YAML file to guide the user.

    Args:
        filepath (str): The path where the template YAML file will be created.
        overwrite (bool): If True, will overwrite an existing file. Defaults to False.
    """
    if os.path.isfile(filepath) and not overwrite:
        logging.warning(
            f"File '{filepath}' already exists. Use overwrite=True to replace it."
        )
        return
    template_content = """
# --- Metadata for the associated data file ---
# Please fill out the values for each field.
# Required fields are marked. Others are optional.
# Date format should be YYYY-MM-DD.

# --- Project & Author (Required) ---
research_project_id: "" # e.g., "Frequency Sweep"
author: ""            # e.g., "wkm2109"

# --- Experiment Details (Optional) ---
experiment_type: ""   # e.g., "Data Calibration"
date_conducted: ""    # e.g., "2025-01-15"

# --- Descriptive Metadata (Optional) ---
custom_tags: ""       # e.g., "1.5 mHZ, 2V, simulation, NHP, etc."
"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            f.write(template_content.strip())
        logging.info(f"Template YAML created at: {filepath}")
    except Exception as e:
        raise FileConfigurationError(f"Could not create template file: {e}")


def upload_file(
    data_file_path: str, metadata_file_path: str, api_url: str = None
) -> dict:
    """
    The core logic for ingesting a file by calling the data management API.

    Args:
        data_file_path (str): The local path to the raw data file.
        metadata_file_path (str): The local path to the YAML metadata file.
        api_url (str, optional): The base URL of the ingestion API.
                                 Defaults to INGEST_API_URL env var or http://localhost:8001.

    Returns:
        A dictionary containing the JSON response from the API on success.

    Raises:
        FileNotFoundError: If either of the input files do not exist.
        FileConfigurationError: If the YAML file is invalid.
        APIError: If the API returns an error.
        requests.exceptions.RequestException: For network-level errors.
    """

    logging.info(f"Starting ingestion for data file: '{data_file_path}'")

    # --- 1. Validate local files and metadata ---
    if not os.path.exists(data_file_path):
        raise FileNotFoundError(f"Data file not found at: {data_file_path}")
    if not os.path.exists(metadata_file_path):
        raise FileNotFoundError(f"Metadata file not found at: {metadata_file_path}")
    try:
        with open(metadata_file_path, "r") as f:
            metadata = yaml.safe_load(f)
        if (
            not isinstance(metadata, dict)
            or not metadata.get("research_project_id")
            or not metadata.get("author")
        ):
            raise FileConfigurationError(
                "Metadata YAML is invalid or missing required keys: 'research_project_id', 'author'."
            )
    except Exception as e:
        raise FileConfigurationError(f"Failed to read or parse YAML file: {e}")

    # --- 2. Prepare and call the API ---
    target_url = api_url or API_BASE_URL
    upload_endpoint = f"{target_url}/uploadfile/"

    files_arguments = {
        "data_file": (os.path.basename(data_file_path), open(data_file_path, "rb")),
        "metadata_file": (
            os.path.basename(metadata_file_path),
            open(metadata_file_path, "rb"),
        ),
    }

    response = None
    try:
        logging.info(f"Calling API at {upload_endpoint}...")
        response = requests.post(
            upload_endpoint, files=files_arguments, timeout=1500
        )  # 5 min timeout
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes

        logging.info("--- Ingestion Successful ---")
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        error_details = http_err.response.text
        try:  # Try to parse JSON for a cleaner error message
            error_details = http_err.response.json()
        except Exception:
            pass
        raise APIError(
            f"API returned an error: {http_err}",
            status_code=http_err.response.status_code,
            details=error_details,
        )

    except requests.exceptions.RequestException as req_err:
        # For connection errors, timeouts, etc.
        logging.error(f"Could not connect to API at {target_url}: {req_err}")
        raise req_err
    finally:
        # Ensure files are closed
        for file_tuple in files_arguments.values():
            file_tuple[1].close()


def search_file(
    research_project_id: str = None,
    author: str = None,
    file_type: str = None,
    experiment_type: str = None,
    tags_contain: str = None,
    date_after: str = None,
    date_before: str = None,
    api_url: str = None,
) -> pd.DataFrame:
    """
    Searches for file metadata via the API and returns the results as a pandas DataFrame.

    Args:
        research_project_id: Filter by an exact research project ID.
        author: Filter by author name (case-insensitive, partial match).
        file_type: Filter by file extension, e.g., 'PDF', 'MAT' (case-insensitive).
        experiment_type: Filter by experiment type (case-insensitive, partial match).
        tags_contain: Search for a keyword within the custom_tags field.
        date_after: Filter for files conducted ON or AFTER this date (YYYY-MM-DD).
        date_before: Filter for files conducted ON or BEFORE this date (YYYY-MM-DD).
        api_url (str, optional): The base URL of the ingestion API. Overrides default.

    Returns:
        A pandas DataFrame containing the search results. Returns an empty DataFrame if no results are found.

    Raises:
        APIError: If the API returns an error.
        requests.exceptions.RequestException: For network-level errors.
    """
    target_url = api_url or API_BASE_URL
    search_endpoint = f"{target_url}/search/"

    params = {
        "research_project_id": research_project_id,
        "author": author,
        "file_type": file_type,
        "experiment_type": experiment_type,
        "tags_contain": tags_contain,
        "date_after": date_after,
        "date_before": date_before,
    }

    active_params = {k: v for k, v in params.items() if v is not None}

    logging.info(f"Querying API at {search_endpoint} with parameters: {active_params}")

    try:
        response = requests.get(search_endpoint, params=active_params)
        response.raise_for_status()  # Raise an exception for 4xx/5xx status codes

        results_json = response.json()

        if not results_json:
            logging.info("Search returned no results.")
            return pd.DataFrame()

        df = pd.DataFrame(results_json)

        for col in ["date_conducted", "upload_timestamp"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if df[col].isna().any():
                    logging.warning(
                        f"Column '{col}' contains invalid dates that were converted to NaT."
                    )

        return df

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred during search: {http_err}")
        try:
            logging.error(f"API Error Details: {http_err.response.json()}")
        except Exception:
            logging.error(
                f"Could not parse error response. Raw text: {http_err.response.text}"
            )
        # Re-raise as your custom APIError if you have one, or just re-raise the requests error
        raise http_err
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Could not connect to API at {target_url}: {req_err}")
        raise req_err

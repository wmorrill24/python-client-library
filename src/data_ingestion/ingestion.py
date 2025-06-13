import os
import yaml
import requests
import logging
from .exceptions import APIError, FileConfigurationError
import pandas as pd
from pathlib import Path
# from rich.console import Console

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
API_BASE_URL = os.getenv("INGEST_API_URL", "http://localhost:8001")


def set_api_url(url: str):
    """
    Sets the base URL for the API for the current session.

    Args:
        url (str): The new base URL to use for API calls.
    """
    global API_BASE_URL
    API_BASE_URL = url
    logging.info(f"API base URL has been set to: {API_BASE_URL}")


def get_api_url() -> str:
    """
    Gets the currently configured base URL for the API.

    Returns:
        The current API base URL.
    """
    return API_BASE_URL


def generate_metadata_template(filepath: str, overwrite: bool = False):
    """
    Generates a blank metadata YAML file to guide the user.

    Args:
        filepath (str): The path where the template YAML file will be created.
        overwrite (bool): If True, will overwrite an existing file. Defaults to False.
    """
    if os.path.exists(filepath) and not overwrite:
        logging.warning(
            f"File '{filepath}' already exists. Use overwrite=True to replace it. Aborting."
        )
        return
    try:
        directory = os.path.dirname(filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)

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
        with open(filepath, "w") as f:
            f.write(template_content.strip())
        logging.info(f"Template YAML created at: {filepath}")
    except Exception as e:
        raise FileConfigurationError(f"Could not create template file: {e}")


def upload_file(data_file_path: str, metadata_file_path: str) -> dict:
    """
    The core logic for ingesting a file by calling the data management API.

    Args:
        data_file_path (str): The local path to the raw data file.
        metadata_file_path (str): The local path to the YAML metadata file.

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
    target_url = API_BASE_URL
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
    file_id: str = None,
    research_project_id: str = None,
    author: str = None,
    file_type: str = None,
    experiment_type: str = None,
    tags_contain: str = None,
    date_after: str = None,
    date_before: str = None,
) -> pd.DataFrame:
    """
    Searches for file metadata via the API and returns the results as a pandas DataFrame.

    Args:
        file_id: Filter by a File's auto-generated UUID
        research_project_id: Filter by an exact research project ID.
        author: Filter by author name (case-insensitive, partial match).
        file_type: Filter by file extension, e.g., 'PDF', 'MAT' (case-insensitive).
        experiment_type: Filter by experiment type (case-insensitive, partial match).
        tags_contain: Search for a keyword within the custom_tags field.
        date_after: Filter for files conducted ON or AFTER this date (YYYY-MM-DD).
        date_before: Filter for files conducted ON or BEFORE this date (YYYY-MM-DD).

    Returns:
        A pandas DataFrame containing the search results. Returns an empty DataFrame if no results are found.

    Raises:
        APIError: If the API returns an error.
        requests.exceptions.RequestException: For network-level errors.
    """
    target_url = API_BASE_URL
    search_endpoint = f"{target_url}/search/"

    params = {
        "file_id": file_id,
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


def download_file(file_id: str, destination_path: str = None) -> None:
    """
    Downloads a file from the data lake using its file_id by calling the backend API.

    This function streams the file directly to disk, making it efficient for large files.

    Args:
        file_id (str): The unique UUID of the file to download.
        destination_path (str) (optional): The local path where the file should be saved.
                                If this path is an existing directory, the file's original
                                name (provided by the server) will be used.
                                If it's a full path including a filename, it will be saved there.

    Returns:
        The final absolute path to the downloaded file on success.

    Raises:
        APIError: If the API returns an error (like 404 Not Found or 500).
        requests.exceptions.RequestException: For network-level errors like connection failures.
        FileNotFoundError: If the destination directory does not exist.
    """
    target_url = API_BASE_URL
    download_endpoint = f"{target_url}/download/{file_id}/"

    if destination_path is None:
        final_dest_path = Path.home() / "Downloads"
        logging.info(
            f"No destination path provided. Defaulting to user's Downloads folder: {final_dest_path}"
        )
    else:
        final_dest_path = Path(destination_path)

    save_directory = (
        final_dest_path if final_dest_path.is_dir() else final_dest_path.parent
    )
    save_directory.mkdir(parents=True, exist_ok=True)

    try:
        # Use streaming to handle potentially large files without loading all into memory
        with requests.get(download_endpoint, stream=True) as r:
            # Check for HTTP errors like 404 Not Found or 500 Internal Server Error from the API
            r.raise_for_status()
            final_save_path = final_dest_path
            if final_dest_path.is_dir():
                content_disp = r.headers.get("content-disposition")
                if content_disp and "filename=" in content_disp:
                    filename_from_header = content_disp.split("filename=")[1].strip('"')
                    final_save_path = final_dest_path / filename_from_header
                else:
                    final_save_path = final_dest_path / f"{file_id}.dat"

            logging.info(f"Downloading file to: {final_save_path}")
            with open(final_save_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

        logging.info("File downloaded successfully!")
        return str(final_save_path)
    except requests.exceptions.HTTPError as http_err:
        # Provide a more user-friendly error message
        error_message = (
            f"API returned an error (Status {http_err.response.status_code})"
        )
        try:
            # Try to get the specific detail message from your API's JSON response
            api_detail = http_err.response.json().get("detail", http_err.response.text)
            error_message += f": {api_detail}"
        except Exception:
            error_message += f": {http_err.response.text}"

        logging.error(error_message)
        # Re-raise your custom APIError or a general Exception so the calling script knows it failed
        raise Exception(error_message) from http_err

    except requests.exceptions.RequestException as req_err:
        logging.error(f"Could not connect to API at {target_url}: {req_err}")
        raise req_err

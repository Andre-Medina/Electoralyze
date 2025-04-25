import logging
import os
from zipfile import ZipFile

import polars as pl
import requests

BASE_TIMEOUT = 60


def create_path(file_path: str, /):
    """Creates a path to a file."""
    if not isinstance(file_path, str):
        raise ValueError("`file_path` must be a string")
    if file_path == "":
        raise ValueError("`file_path` cannot be an empty string")
    if file_path[0] != "/":
        raise ValueError("`file_path` must be an absolute path")
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)


def download_file(url: str, filename: str, *, timeout: int = BASE_TIMEOUT):
    """Download a file from a given URL and save it locally.

    Parameters
    ----------
    url (str): URL of the file to download
    filename (str): Name to save the file as

    Returns
    -------
    bool: True if successful, False otherwise
    """
    create_path(filename)

    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx

    # Open file in binary write mode
    with open(filename, "wb") as file:
        # Download in chunks to handle large files efficiently
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # Filter out keep-alive chunks
                file.write(chunk)


def pl_scan_csv_zip(*, csv_zip_url: str, schema: pl.Schema, add_file_name: bool = False, **scan_kwargs) -> pl.LazyFrame:
    """Scan all CSV files in a combined zip."""
    read_data = []

    with ZipFile(csv_zip_url, "r") as zip:
        for file in zip.filelist:
            logging.info(f"reading: {file.filename!r}")
            if not file.filename.endswith(".csv"):
                raise ValueError("Found non CSV file in zip.")

            file_name_column = pl.lit(file.filename).alias("file_name")

            read_data.append(
                pl.scan_csv(
                    zip.open(file.filename),
                    schema=schema,
                    **scan_kwargs,
                ).with_columns(file_name_column if add_file_name else [])
            )

    combined_data = pl.concat(read_data)

    return combined_data

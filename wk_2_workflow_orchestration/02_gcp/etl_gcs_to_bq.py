#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download data from GCS bucket."""
    local_path = Path("../data")
    local_path.resolve().mkdir(parents=True, exist_ok=True)

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dez-prefect-test")
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)

    return local_path / gcs_path


@flow()
def etl_gcs_to_bq() -> None:
    """ Main ETL function. """

    color = "yellow"
    year, month = (2021, 1)

    path = extract_from_gcs(color, year, month)


if __name__ == '__main__':
    etl_gcs_to_bq()

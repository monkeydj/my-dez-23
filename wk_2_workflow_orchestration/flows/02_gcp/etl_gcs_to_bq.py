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


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dez-prefect-gcp")
    gcp_sa_creds = gcp_credentials_block.get_credentials_from_service_account()

    df.to_gbq(
        destination_table="dez.rides",
        project_id="de-zoomcamp-23-375203",
        credentials=gcp_sa_creds,
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq() -> None:
    """ Main ETL function. """

    color = "yellow"
    year, month = (2021, 1)

    path = extract_from_gcs(color, year, month)
    df = transform(path=path)
    write_bq(df=df)


if __name__ == '__main__':
    etl_gcs_to_bq()

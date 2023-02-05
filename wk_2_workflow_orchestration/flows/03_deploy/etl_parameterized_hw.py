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
    print(df.head(2))
    print(f"columns: {df.columns}")
    print(f"rows: {len(df)}")

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
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    """Main ETL function."""

    path = extract_from_gcs(color, year, month)
    df = transform(path=path)
    write_bq(df=df)

    return df.count()


@flow(log_prints=True)
def elt_wrapped_flow(
    color: str = "yellow", year: int = 2021, months: list[int] = [1, 2]
) -> None:
    data_processed_count = 0
    for month in months:
        data_processed_count += etl_gcs_to_bq(color, year, month)

    print(f"Total processed data: {data_processed_count}")


if __name__ == "__main__":
    color, year, months = ("yellow", 2021, [1, 2, 3])
    elt_wrapped_flow(color=color, year=year, months=months)

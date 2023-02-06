#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials


@flow()
def fetch(color: str, year: int, month: int) -> None:
    """Fetch taxi data from url into Pandas dataframe."""

    dataset_archive = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"{dataset_archive}/{color}/{dataset_file}.csv.gz"

    print(f"Loading data from {dataset_url}...")

    df = pd.read_csv(dataset_url)

    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype at column(6) and (maybe) other issues."""

    # generic solution to all datetime fields
    for col in df.columns:
        if col.endswith("_datetime"):
            df[col] = pd.to_datetime(df[col])

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
        destination_table="dez.rides_hw",
        project_id="de-zoomcamp-23-375203",
        credentials=gcp_sa_creds,
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    """Main ETL function."""

    df = fetch(color, year, month)
    df = transform(df=df)
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

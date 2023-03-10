#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    retries=3,
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(data_url: str) -> pd.DataFrame:
    """Fetch taxi data from url into Pandas dataframe."""
    print(f"Loading data from {data_url}...")
    df = pd.read_csv(data_url)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype at column(6) and (maybe) other issues."""

    # generic solution to all datetime fields
    for col in df.columns:
        if col.endswith("_datetime"):
            df[col] = pd.to_datetime(df[col])

    print(df.head(2))
    print(f"columns: {df.columns}")
    print(f"rows: {len(df)}")

    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, data_file: str) -> Path:
    """Write dataframe as parquet file."""

    file_path = Path(f"data/{color}/{data_file}.parquet")
    file_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing local parquet: {file_path}...")
    df.to_parquet(file_path, compression="gzip")

    return file_path


@task()
def write_gcs(path: Path) -> None:
    """Upload parquet file to GCS."""

    gcs_block = GcsBucket.load("dez-prefect-test")
    gcs_block.upload_from_path(from_path=path, to_path=path)

    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """Main ETL function."""

    dataset_archive = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"{dataset_archive}/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    write_gcs(path=path)


@flow()
def elt_wrapped_flow(
    color: str = "yellow", year: int = 2021, months: list[int] = [1, 2]
) -> None:
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    color, year, months = ("yellow", 2021, [1, 2, 3])
    elt_wrapped_flow(color=color, year=year, months=months)

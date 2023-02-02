#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3, log_prints=True)
def fetch(data_url: str) -> pd.DataFrame:
    """ Fetch taxi data from url into Pandas dataframe. """

    # pseudo failure
    if randint(0, 1) > 0:
        raise Exception

    print(f"Loading data from {data_url}...")
    df = pd.read_csv(data_url)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """ Fix dtype at column(6) and (maybe) other issues. """

    # on yellow taxi data
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    print(df.head(2))
    print(f"columns: {df.columns}")
    print(f"rows: {len(df)}")

    return df


@flow()
def etl_web_to_gcs() -> None:
    """ Main ETL function. """

    color = "yellow"
    year, month = (2021, 1)

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)


if __name__ == '__main__':
    etl_web_to_gcs()

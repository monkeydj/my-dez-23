#!/usr/bin/env python3.10
# coding: utf-8

import pandas as pd
import numpy as np

from math import ceil
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


# Special thanks to super helpful support from @iobruno
# https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674536469497729?thread_ts=1674534733.532899&cid=C01FABYF2RG


def split_df_in_chunks_with(df: pd.DataFrame, chunk_size: int = 1e5):
    chunks_qty = ceil(len(df) / chunk_size)
    return np.array_split(df, chunks_qty), chunks_qty


@task(log_prints=True, retries=3)
def extract_data(csv_url):
    print(f'downloading data from {csv_url}...')
    return pd.read_csv(csv_url, engine='pyarrow')


@task(log_prints=True)
def transform_data(df):

    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True)
def load_data(table_name, df):

    block_conn = SqlAlchemyConnector.load("dez-23-ny-taxi")

    with block_conn.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name='Ingest Green trips')
def main_flow(table_name):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    log_subflow(table_name=table_name)

    data_raw = extract_data(csv_url=csv_url)
    data_stg = transform_data(data_raw)

    load_data(table_name=table_name, df=data_stg)


if __name__ == '__main__':
    main_flow('test_flow_blk_ingest_green_trips')

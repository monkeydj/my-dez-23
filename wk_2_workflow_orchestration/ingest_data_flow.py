#!/usr/bin/env python3.10
# coding: utf-8

import pandas as pd
import numpy as np

from time import time
from sqlalchemy import create_engine
from math import ceil
from prefect import flow, task


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
def load_data(conn, table_name, df):

    engine = create_engine(conn)
    engine.connect()
    chunks, qty = split_df_in_chunks_with(df)

    for i, df_chunk in enumerate(chunks):
        t_start = time()

        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        print(f'inserted chunk #{i+1}/{qty} in %.3f secs' % (t_end - t_start))


@flow(name='Ingest Green trips')
def main_flow(table_name):
    conn = "postgresql://root:root@localhost:5432/ny_taxi"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    data_raw = extract_data(csv_url=csv_url)
    load_data(conn=conn, table_name=table_name, df=data_raw)


if __name__ == '__main__':
    main_flow('test_flow_ingest_green_trips')

#!/usr/bin/env python3.10
# coding: utf-8

import argparse
import pandas as pd
import numpy as np

from time import time
from sqlalchemy import create_engine
from math import ceil


# Special thanks to super helpful support from @iobruno
# https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674536469497729?thread_ts=1674534733.532899&cid=C01FABYF2RG


def split_df_in_chunks_with(df: pd.DataFrame, chunk_size: int = 1e5):
    chunks_qty = ceil(len(df) / chunk_size)
    return np.array_split(df, chunks_qty), chunks_qty


def main(conn, table_name, csv_url):

    engine = create_engine(conn)
    engine.connect()

    df_raw = pd.read_csv(csv_url, engine='pyarrow')
    chunks, qty = split_df_in_chunks_with(df_raw)

    for i, df in enumerate(chunks):
        t_start = time()

        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        print(f'inserted chunk #{i+1}/{qty} took %.3f secs' %
              (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data into Postgres')
    parser.add_argument(
        '--conn', help='full connection string to Postgres instance')
    parser.add_argument(
        '--table_name', help='table name to be imported on Postgres')
    parser.add_argument('--csv_url', help='url of CSV file')

    args = parser.parse_args()
    main(**args)

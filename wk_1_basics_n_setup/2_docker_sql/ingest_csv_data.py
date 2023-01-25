#!/usr/bin/env python3.10
# coding: utf-8

import argparse
import os

from time import time
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from math import ceil

# Special thanks to super helpful support from @iobruno
# https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674536469497729?thread_ts=1674534733.532899&cid=C01FABYF2RG


def split_df_in_chunks_with(df: pd.DataFrame, max_chunk_size: int = 100000):
    chunks_qty = ceil(len(df) / max_chunk_size)
    return np.array_split(df, chunks_qty), chunks_qty


def main(params):
    conn = params.conn
    table_name = params.table_name
    csv_url = params.csv_url  # should be a csv.gz file
    csv_file = f'{os.getcwd()}/data/output.csv.gz'

    # download file on MacOS
    os.system(f"wget {csv_url} -O {csv_file}")

    engine = create_engine(conn)
    engine.connect()

    df = pd.read_csv(csv_file, engine='pyarrow')
    chunks, qty = split_df_in_chunks_with(df)

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
        '--conn', help='connection string to Postgres instance')
    parser.add_argument(
        '--table_name', help='table name to be imported on Postgres')
    parser.add_argument('--csv_url', help='url of CSV file')

    args = parser.parse_args()
    main(args)

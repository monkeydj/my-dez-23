#!/usr/bin/env python3.10
# coding: utf-8

import argparse
import os

from time import time
from sqlalchemy import create_engine
import pandas as pd


def main(params):
    conn = params.conn  # ! must be a full connection string
    table_name = params.table_name
    csv_url = params.csv_url
    csv_file = f'{os.getcwd()}/data/output.csv.gz'

    # download file on MacOS
    os.system(f"wget {csv_url} -O {csv_file}")

    engine = create_engine(conn)
    engine.connect()

    df_iter = pd.read_csv(csv_file,
                          iterator=True, chunksize=1e5)
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # this will create the table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        t_start = time()

        df = next(df_iter)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk took %.3f secs' % (t_end - t_start))


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

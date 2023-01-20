#!/usr/bin/env python3.10
# coding: utf-8

import argparse
import os

from time import time
from sqlalchemy import create_engine
import pyarrow.parquet as pq


def main(params):
    conn = params.conn  # ! must be a full connection string
    table_name = params.table_name
    data_url = params.data_url
    data_output_file = f'{os.getcwd()}/data/output.parquet'

    t_start = time()

    # * download directly from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    os.system(f"curl --progress-bar {data_url} -o {data_output_file}")

    t_end = time()

    print('Dowloaded data in %.3f secs' % (t_end - t_start))

    engine = create_engine(conn)
    engine.connect()

    data_parquet = pq.ParquetFile(data_output_file)
    data_iter = data_parquet.iter_batches(batch_size=1e5)

    for data_batch in data_iter:
        t_start = time()

        df = data_batch.to_pandas()
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted data batch in %.3f secs' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data into Postgres')
    parser.add_argument(
        '--conn', help='connection string to Postgres instance')
    parser.add_argument(
        '--table_name', help='table name to be imported on Postgres')
    parser.add_argument('--data_url', help='url of data file')

    args = parser.parse_args()
    main(args)

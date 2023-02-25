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
    print(f'Loading data from {data_url}...')
    df = pd.read_csv(data_url)

    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype at column(6) and (maybe) other issues."""

    # generic solution to all datetime fields
    for col in df.columns:
        if col.endswith('_datetime'):
            df[col] = pd.to_datetime(df[col])

    print(df.head(2))
    print(f'columns: {df.columns}')
    print(f'rows: {len(df)}')

    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, data_file: str, as_parquet: bool = False) -> Path:
    """Write dataframe to a local file."""

    data_dir = color + '_parquet' if as_parquet else ''
    file_suffix = '.parquet' if as_parquet else '.csv.gz'
    file_path = Path(f'data/{data_dir}/{data_file}').with_suffix(file_suffix)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    print(f'Writing to local file: {file_path.resolve()}...')
    if as_parquet:
        df.to_parquet(file_path, compression='gzip')
    else:
        df.to_csv(file_path, compression='gzip', index=False)

    return file_path


@task()
def write_gcs(path: Path) -> None:
    """Upload a file to GCS."""

    gcs_block = GcsBucket.load('dez-prefect-test')
    gcs_block.upload_from_path(from_path=path, to_path=path)

    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int, as_parquet: bool = False) -> int:
    """Main ETL function."""

    dataset_archive = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'{dataset_archive}/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_transformed = transform(df)

    path = write_local(df_transformed, color, dataset_file, as_parquet)
    write_gcs(path=path)

    return df_transformed.count()


@flow(log_prints=True)
def elt_batch(
    color: str = 'yellow',
    year: int = 2021,
    months: list[int] = [1, 2],
    as_parquet: bool = False
) -> None:
    processed_total = 0
    for month in months:
        processed_total += etl_web_to_gcs(color, year, month, as_parquet)

    print(f'Total processed data: {processed_total}')


if __name__ == '__main__':
    elt_batch(color='fhv', year=2019, months=[1], as_parquet=True)

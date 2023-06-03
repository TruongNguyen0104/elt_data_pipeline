from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import sys
import argparse

@task(retries=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read taxi data from local into pandas DataFrame
    """
    print(dataset_url)
    df = pd.read_parquet(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """
    Fix types issue
    """
    df = df.reset_index(drop=True)
    df["Date"] = pd.to_datetime(df['Date'],format='%m/%d/%Y %I:%M:%S %p')
    df['Updated On'] = pd.to_datetime(df['Updated On'],format='%m/%d/%Y %I:%M:%S %p')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, file_num: int) -> Path:
    """
    Write DataFrame out locally as parquet file
    """

    path = Path(f"/opt/prefect/data/extract/crime_part_{file_num}.parquet")
    df.to_parquet(path,compression='gzip')
    return path

@task()
def write_gcs(path: Path,file_num : int = 0) -> None:
    """
    Uploading a local parquet file to GCS
    """
 
    gcs_block = GcsBucket.load("data-pipeline-bucket")
    gcs_block.upload_from_path(
        from_path =path,
        to_path = f"data/crime_part_{file_num}.parquet"
    )
    return

@flow
def elt_sub_flow_to_gcs(file_num:int = 0) -> None:
    """
    SubFlow from local to gcs
    """

    dataset_url =  f'/opt/prefect/data/raw/part.{file_num}.parquet'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path= write_local(df_clean,file_num)
    write_gcs(path,file_num)

@flow()
def etl_web_to_gcs(file_nums: list = [0]) -> None:
    '''
    The main ETL function
    '''

    for i in file_nums:
        elt_sub_flow_to_gcs(i)

    


if __name__ =="__main__":
    # parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    # parser.add_argument('--parts', help="specify parts to load")

    # nums = list(parser.parse_args().parts)

    nums = [*range(0,16)]
    etl_web_to_gcs(nums)
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(num: int) -> Path:
    """
    Download data trip from GSC
    """
    gcs_path =  f"data/crime_part_{num}.parquet"
    local = f"../data/gcs/"
    gcs_block = GcsBucket.load("data-pipeline-bucket")
    gcs_block.get_directory(
        from_path = gcs_path,
        local_path =local
    )
    return Path(f"../data/gcs/")

@task()
def transform(path: Path) -> pd.DataFrame:
    """
    Data cleaning example
    """

    df= pd.read_parquet(path)

    print("Number of Missing Values in the whole dataset : ", df.isna().sum().sum())
    print(round(((df.shape[0]-df.isna().sum().sum())*100) / df.shape[0],2), 
                    "percentage of the data has been retained.")
    df = df.dropna(how='any')
  
    print("Number of Missing Values in the whole dataset (after processing) : ", df.isna().sum().sum())

    df.drop(['Updated On', 'FBI Code','X Coordinate','Y Coordinate',
             'Primary Type','Case Number','Community Area','Location Description'], axis=1,inplace=True)
    df['Ward'] = df['Ward'].astype(dtype='int64')
    # df['Community Area'] = df['Community Area'].astype(dtype='int64')
    # df['Case Number'] = df['Case Number'].astype(dtype='string')
    # df['Location Description']= df['Location Description'].str.replace('.', '')
    df['Location']= df['Location'].str.strip('()')
    print(df.dtypes)
    return df

@task()
def write_bq(df: pd.DataFrame,num: int) -> None:
    """
    Write dataframe to BigQuery
    """
    
    gcp_credentials_block = GcpCredentials.load("gcs-credential")
    df.to_gbq(
        destination_table = f"crime_all.crime_part_{num}",
        project_id = "data-pipeline-388407",
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = "replace",
        table_schema = [{'name': 'IUCR', 'type': 'STRING'}]
    )
    return 

@flow
def elt_sub_flow_to_bq(num: int = 0) -> None:
    """
    SubFlow from gcs to bq 
    """
    path= extract_from_gcs(num)
    df = transform(path)
    write_bq(df,num)

@flow()
def elt_gcs_to_bq(  
    nums : list = [0]):
    """
    Main ETL flow to load data into Big Query
    """
    for num in nums:
        elt_sub_flow_to_bq(num)


if __name__ =="__main__":
    nums = [*range(0,16)]
    elt_gcs_to_bq(nums)
    
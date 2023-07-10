import traceback, json, pandas as pd
from io import BytesIO
from typing import List
from prefect import task
from prefect.tasks import exponential_backoff
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os
from dotenv import load_dotenv

load_dotenv()

from infra.prefect_infra import GCP_CREDENTIALS_BLOCK_NAME

@task(name="upload_to_gcs", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def upload_to_gcs(data, filename: str, savepath: str, prefect_gcs_block: str, mobile_station=False):
    gcp_cloud_storage_bucket_block = GcsBucket.load(prefect_gcs_block)
    filename = filename.split(" ")[0]
    try:
        if isinstance(data, pd.DataFrame):
            save_path = f"{savepath}/{filename} mobile.parquet" if mobile_station else f"{savepath}/{filename}.parquet"
            print(f"Saving to GCS, dataframe format, path: {save_path}")
            gcp_cloud_storage_bucket_block.upload_from_dataframe(
                df = data,
                to_path = save_path,
                serialization_format='parquet'
            )
        elif isinstance(data, dict):
            file = BytesIO(json.dumps(data).encode())
            save_path = f"{savepath}/{filename} mobile.json" if mobile_station else f"{savepath}/{filename}.json"
            print(f"Saving to GCS, dict format, path: {save_path}")
            gcp_cloud_storage_bucket_block.upload_from_file_object(
                from_file_object = file,
                to_path = save_path
            )
        else:
            raise Exception("Data format is wrong to save")
    except Exception:
        print(traceback.format_exc())
        raise Exception("Error saving to GCS")
    
    
@task(name="load_to_bq", log_prints=True, tags="load_bq")
def load_to_bq(df: pd.DataFrame,  to_path_upload: str, table_schema: List[dict] = None, append: bool = True):
    """
    Uploads dataframe to BigQuery in to_path_upload
    if append is True, appends to existing table. if False, overwrites existing table
    """
    load_method = "append" if append else "replace"
    print(f"Loading to bq {to_path_upload} with method {load_method}")
    gcp_credentials_block = GcpCredentials.load(GCP_CREDENTIALS_BLOCK_NAME)
    df.to_gbq(
        destination_table=to_path_upload,
        project_id=os.getenv("PROJECT_ID"),
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists=load_method,
        location=os.getenv("REGION"),
        table_schema=table_schema
    )
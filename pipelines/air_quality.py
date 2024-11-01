import pandas as pd
from prefect import flow
from datetime import datetime, timedelta
from pipelines.etl.extract.extract_aq import extract_aq
from pipelines.etl.transform.transform_aq import transform_data, try_convert_to_df
from pipelines.etl.load.upload import upload_to_gcs, load_to_bq
from infra.prefect_infra import GCS_AIR_QUALITY_BUCKET_BLOCK_NAME


@flow(name="ELT Air Quality", log_prints=True)
def elt_air_quality(raw_gcs_savepath: str, preproc_gcs_savepath: str,dataset: str, date_start: str, date_end: str, time: str):
    """Runs a flow to extract air quality data from the web, transform it and load it into BigQuery and GCS.
    Flow runs from date_start to date_end inclusive of date_end.

    Args:
        raw_gcs_savepath (str): GCS path to save raw data.
        preproc_gcs_savepath (str): GCS path to save preprocessed data.
        dataset (str): Dataset to load to in BQ.
        date_start (str): Format of YYYY-MM-DD, example: 2022-09-21.
        date_end (str): Format of YYYY-MM-DD, example: 2022-09-21. Results are inclusive of date_end.
        time (str): Format of HHMM, example: 1200.
    """
    datetime_start = datetime.strptime(date_start, "%Y-%m-%d")
    datetime_end = datetime.strptime(date_end, "%Y-%m-%d")
    while datetime_start <= datetime_end:
        date = datetime_start.strftime("%Y-%m-%dT%H:%M:%S")
        print(f"Requesting aq data for {date} {time}")
        
        transformed_df = pd.DataFrame()
        
        for state_id in range(1, 16):
            data = extract_aq(state_id, date)
            
            # opting to not store raw data since it has been low value so far
            # raw_datas.append(data)
            
            # if len(raw_datas) == 10:
            #     print("Upload to GCS raw continous AQ data.")
            #     upload_to_gcs(raw_datas, date, raw_gcs_savepath, GCS_AIR_QUALITY_BUCKET_BLOCK_NAME)
            #     raw_datas = []
            
            print("Transforming continous AQ data for state", state_id)
            transformed_data = transform_data(data, date)
            transformed_df = pd.concat([transformed_df, transformed_data], ignore_index=True)
            
        print("Upload to GCS transformed continous AQ data.")
        upload_to_gcs(transformed_data, date, preproc_gcs_savepath, GCS_AIR_QUALITY_BUCKET_BLOCK_NAME)
        
        print("Loading to bigquery")
        load_to_bq(transformed_data, dataset)
        
        datetime_start += timedelta(days=1)


if __name__ == "__main__":
    # elt_flow("2022-05-29", "2023-04-21", "0000", PROD_DATASET) 
    pass
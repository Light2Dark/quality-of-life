import pytz
import pandas as pd
from prefect import flow
from config import DAILY_AQ_DATA_GCS_SAVEPATH, DAILY_PREPROCESSED_AQ_DATA_GCS_SAVEPATH, DEV_DATASET, PROD_DATASET
from datetime import datetime, timedelta
from pipelines.etl.extract.extract_aq import extract_valid_timed_response
from pipelines.etl.transform.transform_aq import transform_data, try_convert_to_df
from pipelines.etl.load.upload import upload_to_gcs, load_to_bq

@flow(name="elt_flow", log_prints=True)
def elt_flow(date_start: str = None, date_end: str = None, time: str = "0000", dataset: str = DEV_DATASET):
    """Runs a flow to extract air quality data from the web, transform it and load it into BigQuery and GCS.
    Flow runs from date_start to date_end inclusive of date_end.

    Args:
        date_start (str): Format of YYYY-MM-DD, example: 2022-09-21. Default is today's date at 12.00am (Kuala Lumpur time)
        date_end (str): Format of YYYY-MM-DD, example: 2022-09-21. Results are inclusive of date_end. Default is today's date at 12.00am (Kuala Lumpur time)
        time (str): Format of HHMM, example: 1200. Defaults to 0000.
        dataset (str, optional): Dataset to load to in BQ. Defaults to "dev.hourly_air_quality". 
    """
    
    date_start = datetime.now(tz=pytz.timezone('Asia/Kuala_Lumpur')).strftime('%Y-%m-%d') if date_start is None else date_start.strip()
    date_end = datetime.now(tz=pytz.timezone('Asia/Kuala_Lumpur')).strftime('%Y-%m-%d') if date_end is None else date_end.strip()
    time = time.strip()
    
    datetime_start = datetime.strptime(date_start, "%Y-%m-%d")
    datetime_end = datetime.strptime(date_end, "%Y-%m-%d")
    while datetime_start <= datetime_end:
        date = datetime_start.strftime("%Y-%m-%d")
        print(f"Requesting data for {date} {time}")
        
        aq_stations_data_24h = extract_valid_timed_response(date, time, False)
        mobile_continous_aq_stations_data_24h = extract_valid_timed_response(date, time, True)
        
        if not (aq_stations_data_24h or mobile_continous_aq_stations_data_24h):        
            print(f"No data, skipping for {date} {time}")
            datetime_start += timedelta(days=1)
            continue
        
        # processing, uploading raw data and getting transformed data
        df_aq_transformed, df_maq_transformed = None, None
        if aq_stations_data_24h:
            upload_to_gcs(aq_stations_data_24h, date, DAILY_AQ_DATA_GCS_SAVEPATH)
            print("Transforming continous AQ data")
            df_aq = try_convert_to_df(aq_stations_data_24h)
            df_aq_transformed = transform_data(df_aq, date)
        if mobile_continous_aq_stations_data_24h:
            upload_to_gcs(mobile_continous_aq_stations_data_24h, date, DAILY_AQ_DATA_GCS_SAVEPATH, mobile_station=True)
            print("Transforming mobile AQ data")
            df_maq = try_convert_to_df(mobile_continous_aq_stations_data_24h)
            df_maq_transformed = transform_data(df_maq, date)
            
        # uploading transformed data
        if df_aq_transformed is not None and not df_aq_transformed.empty and df_maq_transformed is not None and not df_maq_transformed.empty:
            print("Upload to GCS transformed continous and mobile AQ data. Loading to bigquery")
            df_transformed = pd.concat([df_aq_transformed, df_maq_transformed], ignore_index=True)
            upload_to_gcs(df_transformed, date, DAILY_PREPROCESSED_AQ_DATA_GCS_SAVEPATH)
            load_to_bq(df_transformed, dataset)
        elif df_aq_transformed is not None and not df_aq_transformed.empty:
            print("Upload to GCS transformed continous AQ data. Loading to bigquery")
            upload_to_gcs(df_aq_transformed, date, DAILY_PREPROCESSED_AQ_DATA_GCS_SAVEPATH)
            load_to_bq(df_aq_transformed, dataset)
        elif df_maq_transformed is not None and not df_maq_transformed.empty:
            print("Upload to GCS transformed mobile AQ data. Loading to bigquery")
            upload_to_gcs(df_maq_transformed, date, DAILY_PREPROCESSED_AQ_DATA_GCS_SAVEPATH, mobile_station=True)
            load_to_bq(df_maq_transformed, dataset)
            
        datetime_start += timedelta(days=1)

    
@flow(name="test_elt_flow", log_prints=True)
def test_elt_flow():
    response = extract_valid_timed_response("2017-06-09", "0000", False)
    print(response)


if __name__ == "__main__":
    # elt_flow("2022-05-29", "2023-04-21", "0000", PROD_DATASET) 
    # run_parser()
    elt_flow("2023-05-14", "2023-05-14", "0000")
    pass
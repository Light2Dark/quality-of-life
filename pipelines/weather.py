from pipelines.etl.extract import extract_weather
from pipelines.etl.transform import transform_weather
from pipelines.etl.load import upload
from infra.prefect_infra import GCS_WEATHER_BUCKET_BLOCK_NAME
from prefect import flow
from typing import Tuple, List
import datetime
import pandas as pd

PROD_DATASET = "prod.hourly_weather"
DEV_DATASET = "dev.hourly_weather"

RAW_DATA_GCS_SAVEPATH = "hourly_weather_data"
PREPROCESSEED_DATA_GCS_SAVEPATH = "preprocessed_weather_data"


@flow(name="Extract Weather Data", log_prints=True)
def elt_weather(start_date: str, end_date: str, dataset: str = DEV_DATASET):
    """Extract, transform and load weather data from start_date to end_date into BigQuery & GCS.
    Uploads raw and pre-processed data to GCS, and pre-processed data to BigQuery.

    Args:
        start_date (str): Start date in YYYYMMDD format.
        end_date (str): End date in YYYYMMDD format. End date must be > start date.
        dataset (str, optional): Production or dev_dataset. Defaults to DEV_DATASET.
    """
    df = pd.read_csv("pipelines/stations.csv")
    df.dropna(subset=["ICAO"], inplace=True)
    weather_stations_df = df["ICAO"] + ":9:MY"
    weather_stations_list = weather_stations_df.tolist()
    
    start_datetime = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y%m%d")
    
    if end_datetime < start_datetime:
        raise ValueError("End date must be > start date.")
    
    filename = f"{start_date}_{end_date}"
    
    date_chunks = get_date_chunks(start_datetime, end_datetime)
    for start_date, end_date in date_chunks:           
        combined_weather_data = {}
        for weather_station in weather_stations_list:
            weather_data = extract_weather.extract(start_date, end_date, weather_station)
            combined_weather_data[weather_station] = weather_data
        upload.upload_to_gcs(combined_weather_data, filename, RAW_DATA_GCS_SAVEPATH, GCS_WEATHER_BUCKET_BLOCK_NAME)
        
        df_weather = transform_weather.get_weather_df(combined_weather_data, weather_stations_list)
        df_weather.to_csv("all_weather.csv", index=False)
        upload.upload_to_gcs(df_weather, filename, PREPROCESSEED_DATA_GCS_SAVEPATH, GCS_WEATHER_BUCKET_BLOCK_NAME)
        upload.load_to_bq(df_weather, dataset)
        
        

def get_date_chunks(start_datetime: datetime.datetime, end_datetime: datetime.datetime) -> List[Tuple[str, str]]:
    """Extracts weather data in 31 day chunks as that is the max number of days allowed by the API

    Args:
        start_date (datetime): Start date in datetime format
        end_date (datetime): End date in datetime format

    Returns:
        List(Tuple(str, str)): Chunks of start and end dates in YYYYMMDD format. Eg: [("20200101", "20200131"), ("20200201", "20200229"))]
    """
    
    temp_start_date = start_datetime
    temp_end_date = start_datetime

    chunks = []
    while temp_start_date <= end_datetime:
        temp_end_date = temp_start_date + datetime.timedelta(days=31)
        if temp_end_date > end_datetime:
            temp_end_date = end_datetime

        chunks.append((temp_start_date.strftime("%Y%m%d"), temp_end_date.strftime("%Y%m%d")))
        
        temp_start_date = temp_end_date + datetime.timedelta(days=1)

    return chunks

    
if __name__ == "__main__":
    pass
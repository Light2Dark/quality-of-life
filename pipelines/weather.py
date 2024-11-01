from pipelines.etl.extract import extract_weather
from pipelines.etl.transform import transform_weather
from pipelines.etl.load import upload
from infra.prefect_infra import GCS_WEATHER_BUCKET_BLOCK_NAME
from prefect import flow
from typing import Tuple, List, Optional
from datetime import datetime, timedelta
import pandas as pd

@flow(name="ELT Weather", log_prints=True)
def elt_weather(raw_gcs_savepath: str, preproc_gcs_savepath: str, dataset: str, start_date: str, end_date: str, weather_stations: Optional[List[str]] = None):
    """Extract, transform and load weather data from start_date to end_date into BigQuery & GCS.
    Uploads raw and pre-processed data to GCS, and pre-processed data to BigQuery.

    Args:
        raw_gcs_savepath (str): GCS path to save raw data.
        preproc_gcs_savepath (str): GCS path to save pre-processed data.
        dataset (str, optional): Production or dev_dataset.
        start_date (str): Start date in YYYYMMDD format.
        end_date (str): End date in YYYYMMDD format. End date must be <31 days from start_date.
        weather_stations (List[str], optional): List of weather stations to extract data from. If not provided, all ICAO weather stations in the state_locations.csv file will be used.
    """
    df = pd.read_csv("dbt/seeds/state_locations.csv")
    df.dropna(subset=["ICAO"], inplace=True)
    extension = ":9:MY"
    weather_stations_df = df["ICAO"] + extension
    weather_stations_list = weather_stations_df.tolist()
    
    if weather_stations:
        weather_stations_list = list(map(lambda x: x + extension, weather_stations))
    
    weather_data = extract_weather.extract(start_date, end_date, weather_stations_list)   
         
    filename = f"{start_date}_{end_date}"
    upload.upload_to_gcs(weather_data, filename, raw_gcs_savepath, GCS_WEATHER_BUCKET_BLOCK_NAME)
    
    df_weather = transform_weather.get_weather_df(weather_data, weather_stations_list)
    upload.upload_to_gcs(df_weather, filename, preproc_gcs_savepath, GCS_WEATHER_BUCKET_BLOCK_NAME)
    upload.load_to_bq(df_weather, dataset)


@flow(name="ELT Personal Weather", log_prints=True)
async def elt_pws_weather(raw_gcs_savepath: str, preproc_gcs_savepath: str, dataset: str, start_date: str, end_date: str, weather_stations: Optional[List[str]] = None):
    df = pd.read_csv("dbt/seeds/state_locations.csv", header=0)
    personal_weather_stations = df['PWStation'].dropna().unique()
    
    if weather_stations:
        personal_weather_stations = weather_stations
    
    sd = datetime.strptime(start_date, "%Y%m%d")
    ed = datetime.strptime(end_date, "%Y%m%d")
    
    while sd <= ed:
        date = sd.strftime("%Y%m%d")
        weather_data = await extract_weather.extract_pws(date, personal_weather_stations)
        if bool(weather_data) == False:
            print(f"No data for {date}")
            sd += timedelta(days=1)
            continue 
        
        filename = f"{date}_pws"
        upload.upload_to_gcs(weather_data, filename, raw_gcs_savepath, GCS_WEATHER_BUCKET_BLOCK_NAME)
        
        transformed_weather_data = transform_weather.transform_pws_data(weather_data, personal_weather_stations)
        upload.upload_to_gcs(transformed_weather_data, filename, preproc_gcs_savepath, GCS_WEATHER_BUCKET_BLOCK_NAME)
        upload.load_to_bq(transformed_weather_data, dataset)

        sd += timedelta(days=1)
                

def get_date_chunks(start_datetime: datetime, end_datetime: datetime) -> List[Tuple[str, str]]:
    """Extracts weather data in <31 day chunks as that is the max number of days allowed by the API

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
        temp_end_date = temp_start_date + timedelta(days=30)
        if temp_end_date > end_datetime:
            temp_end_date = end_datetime

        chunks.append((temp_start_date.strftime("%Y%m%d"), temp_end_date.strftime("%Y%m%d")))
        
        temp_start_date = temp_end_date + timedelta(days=1)

    return chunks

    
if __name__ == "__main__":
    pass
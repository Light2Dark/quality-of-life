from pipelines.etl.extract import extract_weather
from pipelines.etl.transform import transform_weather
from pipelines.etl.load import upload
from infra.prefect_infra import GCS_WEATHER_BUCKET_BLOCK_NAME
from prefect import flow

WEATHER_STATIONS_LIST = {
    "WMSA:9:MY": "SULTAN ABDUL AZIZ SHAH INTERNATIONAL AIRPORT STATION",
    # "WMKC:9:MY": "KOTA BHARU AIRPORT STATION",
    "WMKK:9:MY": "KUALA LUMPUR INTERNATIONAL AIRPORT STATION",
}

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
        end_date (str): End date in YYYYMMDD format.
        dataset (str, optional): Production or dev_dataset. Defaults to DEV_DATASET.
    """
    filename = f"{start_date}_{end_date}"
    
    # Combines extractions of diff weather stations
    # {
    #   "WMSA:9:MY": {
    #       "metadata": {..}
    #       "observations": [..]
    #    },
    #   "WMKC:9:MY": {
    #       "metadata": {..}
    #       "observations": [..]
    #    },
    #   ...
    # }
    combined_weather_data = {}
    for weather_station in WEATHER_STATIONS_LIST.keys():
        weather_data = extract_weather.extract(start_date, end_date, weather_station)
        combined_weather_data[weather_station] = weather_data
    upload.upload_to_gcs(combined_weather_data, filename, RAW_DATA_GCS_SAVEPATH, GCS_WEATHER_BUCKET_BLOCK_NAME)
    
    df_weather = transform_weather.get_weather_df(combined_weather_data, WEATHER_STATIONS_LIST.keys(), start_date)
    upload.upload_to_gcs(df_weather, filename, PREPROCESSEED_DATA_GCS_SAVEPATH, GCS_WEATHER_BUCKET_BLOCK_NAME)
    upload.load_to_bq(df_weather, dataset)
    
    
if __name__ == "__main__":
    pass
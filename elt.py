import requests
import pandas as pd
from prefect import task, flow
from prefect.tasks import exponential_backoff
from prefect_gcp.cloud_storage import GcsBucket
import json
from datetime import timedelta, datetime
from config import IN_ORDER_TIMINGS, COLUMNS, URLS, DAILY_AQ_DATA_GCS_SAVEPATH, DAILY_TRANSFORMED_AQ_DATA_GCS_SAVEPATH
from utils import local_extract_test, transforming_dates, get_polutant_unit
import traceback

@flow(name="elt_flow", log_prints=True)
def elt_web_to_gcs(testing = True):
    aq_stations_data_24h, mobile_continous_aq_stations_data_24h = extract(testing)
    
    aq = try_convert_to_df(aq_stations_data_24h)
    maq = try_convert_to_df(mobile_continous_aq_stations_data_24h)
    load_to_gcs(aq, DAILY_AQ_DATA_GCS_SAVEPATH)
    load_to_gcs(maq, DAILY_AQ_DATA_GCS_SAVEPATH, mobile_station=True)
    
    df_aq = transform_data(aq_stations_data_24h)
    df_maq = transform_data(mobile_continous_aq_stations_data_24h)
    df = pd.concat([df_aq, df_maq], ignore_index=True)
    load_to_gcs(df, DAILY_TRANSFORMED_AQ_DATA_GCS_SAVEPATH)
    
@flow(name="extract", log_prints=True)
def extract(testing: bool) -> tuple:
    if testing:
        print("Testing mode")
        aq_stations_data_24h = local_extract_test("tests/aq_stations_data_24h.json")
        mobile_continous_aq_stations_data_24h = local_extract_test("tests/mobile_continous_aq_stations_data_24h.json")
    else:
        aq_stations_data_24h = request_api(URLS[0])
        mobile_continous_aq_stations_data_24h = request_api(URLS[1])
        
    return aq_stations_data_24h, mobile_continous_aq_stations_data_24h

@task(name="request_api", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def request_api(url: str) -> dict:
    print(f"Fetching data from {url}")
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        raise Exception(f"Error in getting request from {url}. Error {e}")

@task(name="convert_to_df", log_prints=True)
def try_convert_to_df(response: dict) -> pd.DataFrame or dict:
    print(f"converting to df")
    try:
        df = pd.DataFrame(response)
        return df
    except Exception:
        print(traceback.format_exc())
        return response


@task(name="load_to_gcs", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def load_to_gcs(data, savepath: str, mobile_station=False):
    gcp_cloud_storage_bucket_block = GcsBucket.load("air-quality")
    yesterday = datetime.today() - timedelta(days=1)
    date_str = yesterday.strftime('%d-%m-%Y')
    
    try:
        if isinstance(data, pd.DataFrame):
            save_path = f"{savepath}/{date_str} mobile.parquet" if mobile_station else f"{savepath}/{date_str}.parquet"
            gcp_cloud_storage_bucket_block.upload_from_dataframe(
                df = data,
                to_path = save_path,
                serialization_format='parquet'
            )
        else:
            print("Saving to GCS, not dataframe format")
            save_path = f"{savepath}/{date_str} mobile" if mobile_station else f"{savepath}/{date_str}"
            gcp_cloud_storage_bucket_block.upload_from_json(
                json_data = data,
                to_path = save_path
            )
    except Exception:
        print(traceback.format_exc())
        raise Exception("Error saving to GCS")
    
    
@task(name="transform_data", log_prints=True)
def transform_data(response: dict) -> pd.DataFrame:
    data = response["24hour_api_apims"]
    timings = data[0]
    assert timings == IN_ORDER_TIMINGS, "Timings are not in order"
    
    df_aq = pd.DataFrame(columns=COLUMNS)
    df_states = pd.Series(dtype=str)
    for state_data in data[1:]:
        state = str(state_data[0]).strip().lower().capitalize()
        station_location = str(state_data[1]).strip()
        for index in range(2, len(state_data)):
            value = state_data[index]
            polutant, unit, value = get_polutant_unit(value)
            df = pd.DataFrame(
                {
                    "city": [station_location],
                    "timestamp": [transforming_dates(timings[index])],
                    "polutant": [polutant],
                    "value": [value],
                    "unit": [unit]
                }
            )
            df_aq = pd.concat([df_aq, df], ignore_index=True)
            df_states = pd.concat([df_states, pd.Series(state)], ignore_index=True)
    
    return df_aq
        
        
if __name__ == "__main__":
    elt_web_to_gcs()



# row 2 of the data[0] is the start of the timings (12.00AM)
# row 2 of the rest of the data is the polutant value


# TO-DO: Save as json file / parquet in GCS raw instead of converting to df
# TO-DO: Separate out extract function
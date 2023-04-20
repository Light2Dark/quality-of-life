import requests
import pandas as pd
from prefect import task, flow
from prefect.tasks import exponential_backoff
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from datetime import timedelta, datetime
from config import IN_ORDER_TIMINGS, URLS, DAILY_AQ_DATA_GCS_SAVEPATH, DAILY_PREPROCESSED_AQ_DATA_GCS_SAVEPATH, DEV_DATASET, PROD_DATASET
from utils import local_extract_test, transforming_dates, build_request, try_convert_to_df
import traceback
import argparse
from io import BytesIO
import json
import pytz
    
@flow(name="extract", log_prints=True)
def extract(testing: bool, date:str=None, time:str=None) -> tuple:
    if testing:
        aq_stations_data_24h = local_extract_test("tests/aq_stations_data_24h.json")
        mobile_continous_aq_stations_data_24h = local_extract_test("tests/mobile_continous_aq_stations_data_24h.json")
    else:
        caq_url = build_request(date, time, mobile=False) if date and time else URLS[0]
        mcaq_url = build_request(date, time, mobile=True) if date and time else URLS[1]
        aq_stations_data_24h = request_api(caq_url)
        mobile_continous_aq_stations_data_24h = request_api(mcaq_url)
        
    return aq_stations_data_24h, mobile_continous_aq_stations_data_24h


@task(name="request_api", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def request_api(url: str) -> dict:
    """Returns response.json() from a url. If the response is 404, it will write the url to a file called unavailable_urls.txt and return None"""
    print(f"Fetching data from {url}")
    try:
        response = requests.get(url)
        if response.status_code == 404:
            with open("flows/unavailable_urls.txt", "a") as f:
                f.write(f"{url}\n")
            return None
        return response.json()
    except Exception as e:
        raise Exception(f"Error in getting request from {url}. Error {e}")


@task(name="upload_to_gcs", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def upload_to_gcs(data, filename: str, savepath: str, mobile_station=False):
    gcp_cloud_storage_bucket_block = GcsBucket.load("air-quality")
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


def get_data_timings(response: dict) -> tuple:
    """Returns data and timings from response. If the key has changed, it will print the first key found and return it's value."""
    if "24hour_api_apims" in response:
        data = response["24hour_api_apims"]
    elif "24hour_api" in response:
        data = response["24hour_api"]
    else:
        key_list = list(response.keys())
        print(f"key has changed, selecting first key: {key_list[0]}")
        data = response[key_list[0]]
    timings = data[0]
    
    return data, timings
   
    
@task(name="transform_data", log_prints=True)
def transform_data(response: dict, date:str) -> pd.DataFrame:
    """Transforms the data from the API into a dataframe with proper info

    Args:
        response (dict): _description_
        date (str): Date used in the request to API. Format of YYYY-MM-DD, example: 2022-09-21
        daily_task (bool, optional): Checks timings' correctness if this operation runs daily at 12.00am. Defaults to True.

    Returns:
        pd.DataFrame: _description_
    """
    print("Transforming data to hourly aq dataframe")
    
    data, timings = get_data_timings(response)
    assert timings == IN_ORDER_TIMINGS, "Timings are not in order"
    
    df_aq = pd.DataFrame(columns=['city', 'datetime', 'value'])
    df_states = pd.Series(dtype=str)
    for state_data in data[1:]:
        state = str(state_data[0]).strip().lower().capitalize()
        station_location = str(state_data[1]).strip()
        for index in range(2, len(state_data)):
            value = state_data[index]
            df = pd.DataFrame(
                {
                    "city": [station_location],
                    "datetime": [transforming_dates(date, timings[index])],
                    "value": [value]
                },
            )
            df = df.astype(str) # bigquery doesn't accept datetime
            df_aq = pd.concat([df_aq, df], ignore_index=True)
            df_states = pd.concat([df_states, pd.Series(state)], ignore_index=True)
    
    return df_aq

@task(name="load_to_bq", log_prints=True, tags="load_bq")
def load_to_bq(df: pd.DataFrame,  to_path_upload: str):
    """Uploads dataframe to BigQuery in to_path_upload"""
    print(f"Loading to bq {to_path_upload}")
    gcp_credentials_block = GcpCredentials.load("sham-credentials")
    df.to_gbq(
        destination_table=to_path_upload,
        project_id="quality-of-life-364309",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
        location="asia-southeast1"
    )
    
    
def retry_diff_times(date: str, mobile_station: bool):
    for time in ["2300", "0000", "0100"]:
        request = build_request(date, time, mobile_station)
        print(f"Requesting different URL: {request}")
        response = request_api(request)
        
        if response is not None:
            data, timings = get_data_timings(response)
            if timings == IN_ORDER_TIMINGS:
                return response
    with open("flows/timing_errors.txt", "a") as f:
        f.write(f"{date} {time} {request}")
    # raise Exception(f"Could not find a valid timing response for {date}")
    return None
    
    
def request_valid_timing_response(date: str, time: str, mobile_station: bool):
    request = build_request(date, time, mobile_station)
    response = request_api(request)
    if response is None:
        return None
    data, timings = get_data_timings(response)
    
    if timings != IN_ORDER_TIMINGS:
        print(f"Timings are not in order, retrying with different times")
        response = retry_diff_times(date, mobile_station)
        return response
    else:
        return response

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
        
        aq_stations_data_24h = request_valid_timing_response(date, time, False)
        mobile_continous_aq_stations_data_24h = request_valid_timing_response(date, time, True)
        
        if not (aq_stations_data_24h or mobile_continous_aq_stations_data_24h):        
            print(f"No data, skipping for {date} {time}")
            datetime_start += timedelta(days=1)
            continue
        
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
    response = request_valid_timing_response("2017-06-09", "0000", False)
    print(response)


def run_parser():
    parser = argparse.ArgumentParser(prog="Air Quality ELT", description="An ELT flow to get air quality data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", type=bool,help="If true, dev dataset is use. Else, prod dataset", required=True)
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=True)
    args = parser.parse_args()

    if args.testing:
        elt_flow(args.start_date, args.end_date, args.time)
    elif args.testing is False:
        elt_flow(args.start_date, args.end_date, args.time, PROD_DATASET)

if __name__ == "__main__":
    elt_flow("2017-01-01", "2023-04-20", "0000", "prod.hourly_air_quality") 
    pass
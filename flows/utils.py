import json
import pandas as pd
from prefect import task
from datetime import datetime, timedelta
import regex as re
from prefect_gcp.cloud_storage import GcsBucket
import numpy as np
import traceback

@task(name="save data parquet", log_prints=True)
def save_parquet(dict_data: dict, filepath: str):
    df = pd.DataFrame(dict_data)
    df.to_parquet(filepath, index=False)

@task(name="save data locally", log_prints=True)
def save_json(json_response: json, filepath: str):
    print(f"Saving json response to local storage {filepath}")
    with open(filepath, "w") as f:
        json.dump(json_response, f)
        
def local_extract_test(fp: str) -> json:
    with open(fp, "r") as f:
        return json.load(f)
    
def transforming_dates(date: str, time: str) -> datetime:
    """Convert time to datetime format based on api_table. 
    
    If date is 2021-05-02,
    1.00AM will return 2021-05-01 0100
    11.00AM will return 2021-05-01 1100
    12.00PM will return 2021-05-01 1200
    1.00PM will return 2021-05-01 1300
    11.00PM will return 2021-05-01 2300
    12.00AM will return 2021-05-02 0000

    Args:
        time (str): Time, eg: 12.00AM
        date (str): Date, eg: 01-05-2021. Format must be YYYY-MM-DD.

    Returns:
        datetime: Datetime format, eg: 2021-05-01 02:00:00
    """    
    datetime_format = '%Y-%m-%d %H:%M%p'
    current_day = datetime.strptime(date, '%Y-%m-%d')
    yesterday = (current_day - timedelta(days=1)).strftime('%Y-%m-%d')
    current_day = current_day.strftime('%Y-%m-%d')
    
    day = current_day if time == "12:00AM" else yesterday
    time = "00:00AM" if time == "12:00AM" else time
    if time.endswith("PM"):
        hour = int(time.split(":")[0])
        hour += 12 if hour != 12 else 0
        time = f"{hour}:{time.split(':')[1]}"
    date_str = f"{day} {time}"
    
    return datetime.strptime(date_str, datetime_format) if date_str else None
    

# TODO: Add unit tests
def get_polutant_unit_value(aq_value: str) -> tuple:
    """Get tuple(polutant, unit, value) from aq_value. Returns (None, None, pd.NA) if N/A

    Args:
        aq_value (str): air quality value

    Returns:
        tuple(str, str, int | pd.NA): polutant, unit, value
    """
    if aq_value == "N/A":
        return (None, None, np.nan)
    text = re.sub(r'\d+', '', aq_value)
    number = re.search(r'\d+', aq_value).group()
    number = pd.to_numeric(number, errors="coerce")
    
    match text:
        case "**":
            return ("PM2.5", "ug/m3", number)
        case "*":
            return ("PM10", "ug/m3", number)
        case "a":
            return ("S02", "ppm", number)
        case "b":
            return ("NO2", "ppm", number)
        case "c":
            return ("03", "ppm", number)
        case "d":
            return ("CO", "ppm", number)
        case "&":
            return ("multi-pollutant", None, number)
        case _:
            return ("unknown", None, number)
    
def download_from_gcs(bucket_path: str, save_path: str):
    gcp_cloud_storage_bucket_block: GcsBucket = GcsBucket.load("air-quality")
    gcp_cloud_storage_bucket_block.download_object_to_path(bucket_path, save_path)
    

def build_request(date: str, time: str, mobile_station: bool) -> str:
    """Returns url for the APIs, based on date and time string applied.

    Args:
        date (str): Date string in the format YYYY-MM-DD.
        time (str): Time string in the format HHMM. Will be rounded to the next hour if minutes are present.
        mobile_station (bool): if True, will return url for mobile_continous station. Else, will return url for continuous station.

    Raises:
        e: Error with date string
        ValueError: Time string is not in the correct format.

    Returns:
        str: url_mobile_aq_stations_request for mobile_station and url_continous_aq_stations_request if not
    """    
    date = datetime.strptime(date, '%Y-%m-%d')
    validate_time_format(time)
    
    hour = int(time[:2])
    minute = int(time[2:])
    if minute > 0:
        hour += 1
    hour = hour % 24
    time = f'{hour:02d}00'
    
    if mobile_station:
        return f"http://apims.doe.gov.my/data/public_v2/MCAQM/mcaqmhours24/{date.year}/{date.month:02d}/{date.day:02d}/{time}.json"
    else:
        return f"http://apims.doe.gov.my/data/public_v2/CAQM/hours24/{date.year}/{date.month:02d}/{date.day:02d}/{time}.json"

    # start for url_mcaqm = http://apims.doe.gov.my/data/public_v2/MCAQM/mcaqmhours24/2018/08/17/0000.json
    # start for url_caqm = http://apims.doe.gov.my/data/public_v2/CAQM/hours24/2017/04/14/0000.json 
    
    
def validate_time_format(time: str):
    if not re.match(r'^\d{4}$', time):
        raise ValueError(f"Time {time} is not in the correct format. Please use HHMM")


def try_convert_to_df(response: dict) -> pd.DataFrame or dict:
    print(f"converting to df")
    try:
        df = pd.DataFrame(response)
        return df
    except Exception:
        print(traceback.format_exc())
        return response
        

if __name__ == "__main__":
    print(transforming_dates("2021-05-21", "12:00AM"))
    # download_from_gcs("daily_aq_data/2017-04-20.json", "test.json")
    pass
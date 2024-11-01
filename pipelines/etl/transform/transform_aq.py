from prefect import task
import pandas as pd
import traceback
from datetime import datetime, timedelta
from pipelines.etl.utils.util_aq import IN_ORDER_TIMINGS, get_data_timings

DATETIME_FORMAT = '%Y-%m-%d %H:%M%p'

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
    
    if 'api_table_hourly' not in response:
        print("Error, api_table_hourly not found")
        return
        
    readings = response['api_table_hourly']
    if len(readings) == 0:
        print(f"Error, no readings found for {date}")
        return
    
    df_aq = pd.DataFrame()
    
    for reading in readings:
        if not isinstance(reading, dict) or (reading['DATETIME'] is None or reading['API'] is None):
            print("Reading is missing information", reading)
            continue
        
        reading_time = reading['DATETIME']
        reading_time = datetime.strptime(reading_time, "%Y-%m-%dT%H:%M:%S")
            
        location = reading['STATION_LOCATION']
        if ',' not in location:
            print(f"Station location does not contain ',', location:{location}")
        else:
            location = location.split(',')[0]
        
        df = pd.DataFrame({
            "location": [location],
            "datetime": [reading_time],
            "value": [reading['API']]
        })
        df = df.astype(str) # bigquery doesn't accept datetime
        df_aq = pd.concat([df_aq, df], ignore_index=True)
        
    return df_aq


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
    
    return datetime.strptime(date_str, DATETIME_FORMAT) if date_str else None


def try_convert_to_df(response: dict) -> pd.DataFrame or dict:
    print(f"converting to df")
    try:
        df = pd.DataFrame(response)
        return df
    except Exception:
        print(traceback.format_exc())
        return response
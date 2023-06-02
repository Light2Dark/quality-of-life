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
                    "location": [station_location],
                    "datetime": [transforming_dates(date, timings[index])],
                    "value": [value]
                },
            )
            df = df.astype(str) # bigquery doesn't accept datetime
            df_aq = pd.concat([df_aq, df], ignore_index=True)
            df_states = pd.concat([df_states, pd.Series(state)], ignore_index=True)
    
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
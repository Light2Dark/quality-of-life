from prefect import task
from typing import List
from datetime import datetime, timedelta
# from transform_aq import DATETIME_FORMAT 
import json
import pandas as pd

@task(name="Transform Weather Data", log_prints=True)
def get_weather_df(weather_data: dict, weather_stations: List[str], start_date: str) -> pd.DataFrame:
    """Transforms and models data into dataframe

    Args:
        weather_data (dict): response after extraction
        weather_stations (List[str]): all the weather stations requested
        start_date (str): start date of the data requested. Format: YYYYMMDD

    Returns:
        pd.DataFrame: combined dataframe of all weather stations data
    """
    df_weather = pd.DataFrame(columns=["datetime", "weather_station", "observation_place", "temperature", "pressure", "wind_speed", "weather_phrase", "dew_point", "relative_humidity", "heat_index"])
    
    for station in weather_stations:
        data = weather_data[station]
        date_time = datetime.strptime(start_date, '%Y%m%d')
        
        observations = data["observations"]
        for obs in observations:
            df = pd.DataFrame(
                {
                    "datetime": [date_time],
                    "weather_station": [data["metadata"].get("location_id", station)],
                    "observation_place": [obs["obs_name"]],
                    "temperature": [str(obs["temp"])],
                    "pressure": [str(obs["pressure"])],
                    "wind_speed": [str(obs["wspd"])],         
                    "weather_phrase": [str(obs["wx_phrase"])],
                    "dew_point": [str(obs["dewPt"])],
                    "relative_humidity": [str(obs["rh"])],
                    "heat_index": [str(obs["heat_index"])]
                }
            )
            df_weather = pd.concat([df_weather, df], ignore_index=True)
            date_time += timedelta(hours=1) # each observation is 1 hour apart. Starts from 12am at start_date
        
    df_weather = df_weather.astype(str)
    return df_weather


def get_location_name(weather_station: str) -> str:
    """Get location name from weather station name.
    
    Args:
        weather_station (str): Weather
    """
    return weather_station

def convert_dtypes(df) -> pd.DataFrame:
    convert_dict = {
        "datetime": "datetime64[ns]",
        "weather_station": str,
        "observation_place": str,
        "temperature": float,
        "pressure": float,
        "wind_speed": float,
        "weather_phrase": str,
        "dew_point": float,
        "relative_humidity": float,
        "heat_index": float
    }
    return df.astype(convert_dict)


if __name__ == "__main__":
    with open("tests/combined_weather_data.json", "r") as f:
        data = json.load(f)
        print(get_weather_df(data, ["WMSA:9:MY", "WMKK:9:MY"], "20210101"))
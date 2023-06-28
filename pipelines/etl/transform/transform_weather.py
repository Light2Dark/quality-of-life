from prefect import task
from typing import List
from datetime import datetime
# from transform_aq import DATETIME_FORMAT 
import json
import pandas as pd

@task(name="Transform Weather Data", log_prints=True)
def get_weather_df(weather_data: dict, weather_stations: List[str]) -> pd.DataFrame:
    """Transforms and models data into dataframe

    Args:
        weather_data (dict): response after extraction
        weather_stations (List[str]): all the weather stations requested

    Returns:
        pd.DataFrame: combined dataframe of all weather stations data
    """
    df_weather = pd.DataFrame()
    
    for station in weather_stations:
        data = weather_data.get(station, None)
        if data is None:
            print(f"Unable to transform data for station {station}, no data available")
            continue
        
        observations = data["observations"]
        for obs in observations:
            
            weather_station = obs.get("obs_id", obs.get("key", "Unidentified"))
            weather_station = "Unidentified" if weather_station == "96535" else weather_station # 96535 is an unidentified station
            if weather_station != station.split(":")[0] and weather_station != "Unidentified":
                raise ValueError(f"Station {station} does not match observation {weather_station}")
                
            df = pd.DataFrame(
                {
                    "datetime": [datetime.fromtimestamp(obs["valid_time_gmt"])],
                    "class": [str(obs["class"])],
                    "expire_time_gmt": [datetime.fromtimestamp(obs["expire_time_gmt"])],
                    "weather_station": [weather_station],
                    "observation_place": [obs["obs_name"]],
                    "temperature": [str(obs["temp"])],
                    "min_temperature": [str(obs["min_temp"])],
                    "max_temperature": [str(obs["max_temp"])],
                    "feels_like_temperature": [str(obs["feels_like"])],
                    "pressure": [str(obs["pressure"])],
                    "pressure_tend": [str(obs["pressure_tend"])],
                    "pressure_desc": [str(obs["pressure_desc"])],
                    "wind_speed": [str(obs["wspd"])],
                    "wind_direction_deg": [str(obs["wdir"])],
                    "wind_direction_dir": [str(obs["wdir_cardinal"])],
                    "wind_chill": [str(obs["wc"])],
                    "gust": [str(obs["gust"])],
                    "weather_phrase": [str(obs["wx_phrase"])],
                    "dew_point": [str(obs["dewPt"])],
                    "relative_humidity": [str(obs["rh"])],
                    "clouds": [str(obs["clds"])],
                    "precipitation_total": [str(obs["precip_total"])],
                    "precipitation_hourly": [str(obs["precip_hrly"])],
                    "heat_index": [str(obs["heat_index"])],
                    "uv_description": [str(obs["uv_desc"])],
                    "uv_index": [str(obs["uv_index"])],
                    "visibility": [str(obs["vis"])],
                    "water_temperature": [str(obs["water_temp"])],
                    "day_indicator": [str(obs["day_ind"])],
                    "qualifier": [str(obs["qualifier"])],
                    "qualifier_severity": [str(obs["qualifier_svrty"])],
                    "blunt_phrase": [str(obs["blunt_phrase"])],
                    "terse_phrase": [str(obs["terse_phrase"])],
                    "wx_icon": [str(obs["wx_icon"])],
                    "icon_extended": [str(obs["icon_extd"])],
                }
            )
            df_weather = pd.concat([df_weather, df], ignore_index=True)
        
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
        print(get_weather_df(data, ["WMSA:9:MY", "WMKK:9:MY"]))
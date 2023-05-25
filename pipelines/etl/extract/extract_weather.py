from prefect import task
from prefect.tasks import exponential_backoff
import datetime, requests, os, json
from dotenv import load_dotenv
from typing import List

load_dotenv()

DEFAULT_URL = "https://api.weather.com/v1/location/WMSA:9:MY/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=m&startDate=20230521&endDate=20230521"

def convert_to_datetime(timestamp):
    # Convert timestamp to datetime object
    datetime_object = datetime.datetime.fromtimestamp(timestamp)

    # Get current time in local time zone
    current_time = datetime.datetime.now(datetime_object.tzinfo)

    return current_time

def request_url(weather_station: str, date_start: str, date_end: str, unit: str) -> requests.Response:
    """Makes a request to the Weather API.

    Args:
        weather_station (str): A symbol code for a weather station. Eg: WMSA:9:MY -> Subang Intl. Airport
        date_start (str): Start date of the historical data. Format: YYYYMMDD
        date_end (str): End date of the historical data. Format: YYYYMMDD
        unit (str): Either "m" for metric or "e" for imperial.

    Returns:
        requests.Response: Returns response object from the Weather API.
    """
    if unit not in ["m", "e"]:
        raise ValueError("Unit must be either 'm' for metric or 'e' for imperial.")
    
    url = f"https://api.weather.com/v1/location/{weather_station}/observations/historical.json?apiKey={os.getenv('WEATHER_API')}&units={unit}&startDate={date_start}&endDate={date_end}"
    print(f"Requesting {url}")
    
    return requests.get(url)
    
@task(name="Extract Weather Data", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=30))
def extract(date_start: str, date_end: str, weather_station: str) -> dict:
    """Extracts observations from Weather API. Adds weather station to 

    Args:
        date_start (str): Start date of the historical data. Format: YYYYMMDD
        date_end (str): End date of the historical data. Format: YYYYMMDD
        weather_station (str): A symbol code for a weather station. Eg: WMSA:9:MY -> Subang Intl. Airport

    Returns:
        List[dict]: An array of observations from Weather API. An observation starts from 12.00am on start_date and the next observation is 1 hour after and keeps continuing.
    """
    
    response = request_url(weather_station, date_start, date_end, "m")
    
    # check response
    response.raise_for_status()
    
    # validate response
    response_json = response.json()
    if response_json["metadata"]["status_code"] != 200:
        raise ValueError("API response unsuccessful.")
    elif response_json["metadata"]["language"] != "en-US":
        raise ValueError("API response language not in English.")
    elif response_json["metadata"]["units"] != "m":
        raise ValueError("API response units not in metric.")
    
    # add weather station name to response
    response_json["weather_station"] = weather_station
    
    return response_json


def extract_local(filepath: str) -> json:
    with open(filepath, "r") as f:
        data = json.load(f)
        observations = data["observations"]
        print(len(observations))        
        
        return observations

if __name__ == "__main__":
    # extract_local("pipelines/etl/extract/temp.json")
    # request_url("WMSA:9:MY", "20230521", "20230520", "m")
    extract("20230521", "20230521", "WMSA:9:MY")

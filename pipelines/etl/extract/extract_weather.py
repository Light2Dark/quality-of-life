from prefect import task, flow
from prefect.tasks import exponential_backoff
from dotenv import load_dotenv
# from infra.prefect_infra import WEATHER_API_SECRET_BLOCK
# from prefect.blocks.system import Secret
import datetime, requests, os, json, asyncio, pandas as pd

load_dotenv()

# weather_api_key = os.getenv("WEATHER_API", Secret.load(WEATHER_API_SECRET_BLOCK).get())
weather_api_key = os.getenv("WEATHER_API")

def convert_to_datetime(timestamp):
    # Convert timestamp to datetime object
    datetime_object = datetime.datetime.fromtimestamp(timestamp)

    # Get current time in local time zone
    current_time = datetime.datetime.now(datetime_object.tzinfo)

    return current_time

def build_request(weather_station: str, date_start: str, date_end: str, unit: str) -> str:
    """Builds the API url

    Args:
        weather_station (str): A symbol code for a weather station. Eg: WMSA:9:MY -> Subang Intl. Airport
        date_start (str): Start date of the historical data. Format: YYYYMMDD
        date_end (str): End date of the historical data. Format: YYYYMMDD. End date must be <31 days from start date.
        unit (str): Either "m" for metric or "e" for imperial.

    Returns:
        str: URL string to make a request to the API
    """
    if unit not in ["m", "e"]:
        raise ValueError("Unit must be either 'm' for metric or 'e' for imperial.")
    
    start_datetime = datetime.datetime.strptime(date_start, "%Y%m%d")
    end_datetime = datetime.datetime.strptime(date_end, "%Y%m%d")
    
    if (end_datetime - start_datetime) > datetime.timedelta(days=31):
        raise ValueError("End date must be <31 days from start date.")
    
    url = f"https://api.weather.com/v1/location/{weather_station}/observations/historical.json?apiKey={weather_api_key}&units={unit}&startDate={date_start}&endDate={date_end}"
    return url

@flow(name="Extract Weather Data", log_prints=True)
async def extract(start_date, end_date, weather_stations_list) -> dict:
    """Extracts weather data from Weather API, returns all the combined weather data in a dictionary. Starting point of the ELT pipeline.
    
    Args:
        date_start (str): Start date of the historical data. Format: YYYYMMDD
        date_end (str): End date of the historical data. Format: YYYYMMDD
        weather_stations_list (str): 

    Returns:
        dict: A dictionary of weather stations and their observations.
    """
    combined_weather_data = {}
    coros = [request_data(start_date, end_date, weather_station) for weather_station in weather_stations_list]
    responses = await asyncio.gather(*coros)
    
    for response in responses:
        if response is None:
            continue # If weather data is unavailable, skip
        combined_weather_data[response["weather_station"]] = response
        
    return combined_weather_data

    
@task(name="Request Weather Data", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=30))
async def request_data(date_start: str, date_end: str, weather_station: str) -> dict | None:
    """
    Builds the API url and makes a request to the API. If the request is successful, returns the response in json. If not, returns None.
    
    Args:
        date_start (str): Start date of the historical data. Format: YYYYMMDD
        date_end (str): End date of the historical data. Format: YYYYMMDD
        weather_station (str): A symbol code for a weather station. Eg: WMSA:9:MY -> Subang Intl. Airport

    Returns:
        List[dict] | None: An array of observations from Weather API. An observation starts from 12.00am on start_date and the next observation is 1 hour after and keeps continuing. If no data, return None
    """
    
    request_url = build_request(weather_station, date_start, date_end, "m")
    response = requests.get(request_url)
    print(f"Requesting {request_url}")
    
    # check response
    try:
        response.raise_for_status()
    except:
        print(f"Error with {weather_station} on {date_start} to {date_end}")
        with open("logs/unavailable_weather_urls.txt", "a") as f:
            request_url = request_url.replace(weather_api_key, "API_KEY")
            f.write(f"{request_url}\n")
        return None
    
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

    
@flow(name="Extract Personal Weather Stations", log_prints=True)
async def extract_pws(date: str, personal_weather_stations) -> dict:
    """Extracts data from personal weather stations day by day and compiles them.

    Args:
        date (str): date of the data to be extracted. Format: YYYYMMDD
        personal_weather_stations (List[str]): list of personal weather stations
        
    Returns:
        dict: data from personal weather stations in dict format
    """
    coros = [request_pws_data(personal_weather_station, date) for personal_weather_station in personal_weather_stations]
    responses = await asyncio.gather(*coros)
    
    combined_weather_data = {}
    for response in responses:
        if response is None:
            continue
        combined_weather_data[response["weather_station"]] = response
        
    return combined_weather_data
        

@task(name="Request personal weather station data", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=30))
async def request_pws_data(personal_weather_station: str, date: str) -> dict | None:
    url = f"https://api.weather.com/v2/pws/history/all?stationId={personal_weather_station}&format=json&units=m&date={date}&numericPrecision=decimal&apiKey={weather_api_key}"
    print(f"Requesting {url}")
    response = requests.get(url)
    
    try:
        response.raise_for_status()
    except:
        print(f"Error with {url}")
        with open("logs/unavailable_weather_pws_urls.txt", "a") as f:
            url = url.replace(weather_api_key, "API_KEY")
            f.write(f"{url}\n")
        return None
    
    # validate response
    response_json = response.json()
    
    # check if empty
    if len(response_json["observations"]) == 0:
        print("Empty response")
        with open("logs/unavailable_weather_pws_urls.txt", "a") as f:
            url = url.replace(weather_api_key, "API_KEY")
            f.write(f"{url}\n")
        return None

    # add weather station name to response
    response_json["weather_station"] = personal_weather_station
    return response_json

def extract_local(filepath: str) -> json:
    with open(filepath, "r") as f:
        data = json.load(f)
        print("num of stations", len(data))
        print("len observations", len(data["WMKK:9:MY"]["observations"]))
        
if __name__ == "__main__":
    extract_local("json_response.json")
    # request_url("WMSA:9:MY", "20230521", "20230520", "m")
    # extract("20230521", "20230521", "WMSA:9:MY")
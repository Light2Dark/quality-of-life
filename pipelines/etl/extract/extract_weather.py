from prefect import task
from prefect.tasks import exponential_backoff
import datetime, requests, os, json
from dotenv import load_dotenv
from infra.prefect_infra import WEATHER_API_SECRET_BLOCK
from prefect.blocks.system import Secret

load_dotenv()

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
    
    weather_api_key = os.getenv("WEATHER_API", Secret.load(WEATHER_API_SECRET_BLOCK).get())
    
    url = f"https://api.weather.com/v1/location/{weather_station}/observations/historical.json?apiKey={weather_api_key}&units={unit}&startDate={date_start}&endDate={date_end}"
    return url
    
@task(name="Extract Weather Data", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=30))
def extract(date_start: str, date_end: str, weather_station: str) -> dict | None:
    """Extracts observations from Weather API. Adds weather station to 

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
            request_url = request_url.replace(os.getenv("WEATHER_API"), "API_KEY")
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

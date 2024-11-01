import requests, json
import regex as re
from prefect import task
from prefect.tasks import exponential_backoff

from pipelines.etl.utils.util_aq import IN_ORDER_TIMINGS, get_data_timings
from datetime import datetime

DEFAULT_URLS = [
    "http://apims.doe.gov.my/data/public_v2/CAQM/last24hours.json",
    "http://apims.doe.gov.my/data/public_v2/MCAQM/lastmcaqm24hours.json",
]


@task(name="extract_aq", log_prints=True)
def extract_aq(state_id: int, time: str) -> dict:
    """Extracts air quality data from the APIMS API.

    Args:
        time (str): Date string in the format YYYY-MM-DD
    """
    
    url = f"https://eqmp.doe.gov.my/api2/publicportalapims/apitablehourly?stateid={state_id}&datetime={time}"
    print("Requesting", url)
    response = requests.get(url, verify=False)
    response.raise_for_status()
    
    data = response.json()
    return data
    

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
        sample for url_mcaqm = http://apims.doe.gov.my/data/public_v2/MCAQM/mcaqmhours24/2018/08/17/0000.json
        sample for url_caqm = http://apims.doe.gov.my/data/public_v2/CAQM/hours24/2017/04/14/0000.json 
    """    
    date = datetime.strptime(date, '%Y-%m-%d')
    
    if not re.match(r'^\d{4}$', time):
        raise ValueError(f"Time {time} is not in the correct format. Please use HHMM")
    
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
    

def local_extract_test(fp: str) -> json:
    with open(fp, "r") as f:
        return json.load(f)
    
    
@task(name="test_extract", log_prints=True)
def test_extract(testing: bool, date:str=None, time:str=None) -> tuple:
    if testing:
        aq_stations_data_24h = local_extract_test("/tests/aq_stations_data_24h.json")
        mobile_continous_aq_stations_data_24h = local_extract_test("/tests/mobile_continous_aq_stations_data_24h.json")
    else:
        caq_url = build_request(date, time, mobile=False) if date and time else DEFAULT_URLS[0]
        mcaq_url = build_request(date, time, mobile=True) if date and time else DEFAULT_URLS[1]
        aq_stations_data_24h = request_api(caq_url)
        mobile_continous_aq_stations_data_24h = request_api(mcaq_url)
        
    return aq_stations_data_24h, mobile_continous_aq_stations_data_24h


def extract_valid_timed_response(date: str, time: str, mobile_station: bool):
    """Returns a valid response from the API. If the timings are not in order, it will retry with different times."""
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


@task(name="request_api", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def request_api(url: str) -> dict:
    """Returns response.json() from a url. If the response is 404, it will write the url to a file called unavailable_urls.log and return None"""
    print(f"Fetching data from {url}")
    try:
        response = requests.get(url)
        if response.status_code == 404:
            print("Data not found")
        return response.json()
    except Exception as e:
        raise Exception(f"Error in getting request from {url}. Error {e}")
    

def retry_diff_times(date: str, mobile_station: bool):
    for time in ["2300", "0000", "0100"]:
        request = build_request(date, time, mobile_station)
        print(f"Requesting different URL: {request}")
        response = request_api(request)
        
        if response is not None:
            data, timings = get_data_timings(response)
            if timings == IN_ORDER_TIMINGS:
                return response
    with open("flows/timing_errors.log", "a") as f:
        f.write(f"{date} {time} {request} \n")
    raise Exception(f"Could not find a valid timing response for {date}")
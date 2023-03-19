import requests
import pandas as pd
from prefect import task, flow
from prefect.tasks import exponential_backoff
import json
from config import IN_ORDER_TIMINGS, COLUMNS, URLS
from utils import local_extract_test, transforming_dates

@flow(name="etl_flow", log_prints=True)
def etl_web_to_gcs(testing = True):
    if testing:
        print("Testing mode")
        aq_stations_data_24h = local_extract_test("tests/aq_stations_data_24h.json")
        mobile_continous_aq_stations_data_24h = local_extract_test("tests/mobile_continous_aq_stations_data_24h.json")
        # save_json(aq_stations_data_24h, "aq_stations_data_24h.json")
        # save_json(mobile_continous_aq_stations_data_24h, "mobile_continous_aq_stations_data_24h.json")
    else:
        aq_stations_data_24h = request_api(URLS[0])
        mobile_continous_aq_stations_data_24h = request_api(URLS[1])
    
    aq_data = transform_data(aq_stations_data_24h)
    print(aq_data)
            
@task(name="request_api", log_prints=True, retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=20))
def request_api(url: str) -> dict:
    print(f"Fetching data from {url}")
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        raise Exception(f"Error in getting request from {url}. Error {e}")
        
@task(name="transform data", log_prints=True)
def transform_data(response: json) -> pd.DataFrame:
    data = response["24hour_api_apims"]
    timings = data[0]
    assert timings == IN_ORDER_TIMINGS, "Timings are not in order"
    
    df_aq = pd.DataFrame(columns=COLUMNS)
    df_states = pd.Series(dtype=str)
    for state_data in data[1:]:
        state = str(state_data[0]).strip().lower().capitalize()
        station_location = str(state_data[1]).strip()
        for index in range(2, len(state_data)):
            polutant = "nitrous"
            value = state_data[index]
            df = pd.DataFrame(
                {
                    "city": [station_location],
                    "timestamp": [transforming_dates(timings[index])],
                    "polutant": [polutant],
                    "value": [value],
                    "unit": ["ug/m3"]
                }
            )
            df_aq = pd.concat([df_aq, df], ignore_index=True)
            df_states = pd.concat([df_states, pd.Series(state)], ignore_index=True)
    
    return df_aq
    
@task(name="save_data_locally", log_prints=True)
def save_json(json_response: json, filepath: str):
    print(f"Saving json response to local storage {filepath}")
    with open(filepath, "w") as f:
        json.dump(json_response, f)
        
if __name__ == "__main__":
    etl_web_to_gcs()



# row 2 of the data[0] is the start of the timings (12.00AM)
# row 2 of the rest of the data is the polutant value
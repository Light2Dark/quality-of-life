import requests
import pandas as pd
import json


def extract_air_quality_data():
    urls = [
        "http://apims.doe.gov.my/data/public_v2/CAQM/last24hours.json",
        "http://apims.doe.gov.my/data/public_v2/MCAQM/lastmcaqm24hours.json",
    ]

    response = requests.get(urls[0])
    air_quality_data = response.json()
    with open("air_quality.json", "w", encoding="utf-8") as f:
        json.dump(air_quality_data, f, ensure_ascii=False, indent=4)


def store_csv(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False)


if __name__ == "__main__":
    file = open("air_quality.json", "r")
    air_quality_data = json.load(file)

    data_arr = air_quality_data["24hour_api_apims"]
    df = pd.DataFrame(data_arr[1:], columns=data_arr[0])

    df_store_locations = pd.DataFrame(df.iloc[:, 0:2])

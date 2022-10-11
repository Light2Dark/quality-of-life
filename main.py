import requests
import pandas as pd
import json
from datetime import datetime
import pytz
from bigquery import save_air_quality_bq, save_state_locs_bq
from utils import timeConversion


def extract_air_quality_data():
    urls = [
        "http://apims.doe.gov.my/data/public_v2/CAQM/last24hours.json",
        "http://apims.doe.gov.my/data/public_v2/MCAQM/lastmcaqm24hours.json",
    ]

    response = requests.get(urls[0])
    air_quality_data = response.json()

    return air_quality_data


def store_csv(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False)


def load_json(filename: str):
    with open(filename, "r") as f:
        data = json.load(f)
    return data


def save_json(data, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    air_quality_data = extract_air_quality_data()
    # air_quality_data = load_json("air_quality.json")

    data_arr = air_quality_data["24hour_api_apims"]
    df = pd.DataFrame(data_arr[1:], columns=data_arr[0])

    # Converting data types from object to string
    df = df.astype("string")

    # converting column names from 12 hour time to 24 hour time
    # df2 = df.loc[:, ~df.columns.isin(["State", "Location"])].applymap(timeConversion)
    df2 = (
        df.loc[:, ~df.columns.isin(["State", "Location"])]
        .columns.to_series()
        .apply(timeConversion)
    )

    def renameColumns(colName):
        if colName in ["State", "Location", "Date"]:
            return colName
        else:
            col = timeConversion(colName)
            return (
                "_" + col
            )  # bigquery fields need to start with an underscore or letter

    df.rename(columns=lambda x: renameColumns(x), inplace=True)

    # adding a date column to the dataframe
    df["Date"] = pd.to_datetime("today").strftime("%d-%m-%Y")
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")

    # simple data modelling
    # first table has locations, air quality data per day and date
    df_locations_air_quality = df.iloc[:, 1:]

    # second table has states and locations
    df_state_locations = pd.DataFrame(df.iloc[:, 0:2])

    # store_csv(df_locations_air_quality, "air_quality_test.csv")
    # store_csv(df_state_locations, "state_locations_test.csv")

    save_air_quality_bq(df_locations_air_quality) # save air_quality data to bigquery
    # save_state_locs_bq(df_state_locations)
    
    # print(len(df.axes[0]))  # num of rows

import requests
import pandas as pd
import json
from bigquery import save_air_quality_bq, save_state_locs_bq
from utils import timeConversion


def get_request(url: str) -> json:
    try:
        response = requests.get(url)
        return response.json()
    except:
        raise Exception("Error in getting request from {}".format(url))


def store_csv(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False)


def load_json(filename: str):
    with open(filename, "r") as f:
        data = json.load(f)
    return data


def save_json(data, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def renameColumns(colName):
    if colName in ["State", "Location", "Date"]:
        return colName
    else:
        col = timeConversion(colName)
        return "_" + col  # bigquery fields need to start with an underscore or letter


def transform_request(data) -> pd.DataFrame:
    data_arr = data["24hour_api_apims"]
    df = pd.DataFrame(data_arr[1:], columns=data_arr[0])

    # Converting data types from object to string
    df = df.astype("string")

    # converting column names from 12 hour time to 24 hour time
    df.rename(columns=lambda x: renameColumns(x), inplace=True)

    # converting column names from 12 hour time to 24 hour time
    # df2 = df.loc[:, ~df.columns.isin(["State", "Location"])].applymap(timeConversion)
    # df2 = (
    #     df.loc[:, ~df.columns.isin(["State", "Location"])]
    #     .columns.to_series()
    #     .apply(timeConversion)
    # )

    # adding a date column to the dataframe
    # yesterday = (pd.to_datetime("today") - pd.Timedelta(days=1)).strftime("%d-%m-%Y")
    df["Date"] = pd.to_datetime("today").strftime(
        "%d-%m-%Y"
    )  # data collected is past 24 hours
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")

    return df


if __name__ == "__main__":
    urls = [
        "http://apims.doe.gov.my/data/public_v2/CAQM/last24hours.json",
        "http://apims.doe.gov.my/data/public_v2/MCAQM/lastmcaqm24hours.json",
    ]

    air_quality_data = get_request(urls[0])
    additional_stations = get_request(urls[1])
    # air_quality_data = load_json("air_quality.json")

    df = transform_request(air_quality_data)

    # Simple data modelling
    # first table has locations, air quality data per day and date
    df_locations_air_quality = df.iloc[:, 1:]

    # second table has states and locations
    df_state_locations = pd.DataFrame(df.iloc[:, 0:2])

    # Additional Stations from second URL
    df_add_stations = transform_request(additional_stations)
    df_add_air_quality = df_add_stations.iloc[:, 1:]
    df_add_states = pd.DataFrame(df_add_stations.iloc[:, 0:2])

    df_locations_air_quality = pd.concat(
        [df_locations_air_quality, df_add_air_quality], axis=0, ignore_index=True
    )
    df_state_locations = pd.concat(
        [df_state_locations, df_add_states], axis=0, ignore_index=True
    )

    # store_csv(df_locations_air_quality, "air_quality_test.csv")
    # store_csv(df_state_locations, "state_locations_test.csv")

    save_air_quality_bq(df_locations_air_quality)           # save air_quality data to bigquery
    # save_state_locs_bq(df_state_locations)                # save state_locations data to bigquery

    # print(len(df.axes[0]))  # num of rows

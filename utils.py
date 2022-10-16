import requests
import json
import pandas as pd


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


def timeConversion(time: str) -> str:
    """Converts 12 hour time to 24 hour time
    Example: 12:00PM -> 2400
    Example: 3.00PM -> 1500
    """
    parts = time.split(":")
    lastPart = parts[1]
    firstPart = parts[0]

    frontResult = ""
    backResult = lastPart[:-2]

    if lastPart[-2:].upper() == "AM":
        frontResult = ("0" + firstPart) if len(firstPart) == 1 else firstPart
        if firstPart == "12":
            frontResult = "00"
    elif lastPart[-2:].upper() == "PM":
        frontResult = str(int(firstPart) + 12)
    else:
        print("Error: Invalid time format")
        return

    return frontResult + backResult

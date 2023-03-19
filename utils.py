import json
import pandas as pd
from prefect import task
from datetime import datetime, timedelta
import regex as re
from prefect_gcp.cloud_storage import GcsBucket

@task(name="save data parquet", log_prints=True)
def save_parquet(dict_data: dict, filepath: str):
    df = pd.DataFrame(dict_data)
    df.to_parquet(filepath, index=False)

@task(name="save data locally", log_prints=True)
def save_json(json_response: json, filepath: str):
    print(f"Saving json response to local storage {filepath}")
    with open(filepath, "w") as f:
        json.dump(json_response, f)
        
def local_extract_test(fp: str) -> json:
    with open(fp, "r") as f:
        return json.load(f)
    
    
def transforming_dates(time: str) -> datetime:
    """Convert time to datetime format using today's date

    Args:
        time (str): Time, eg: 12.00AM

    Returns:
        datetime: Datetime format, eg: 2021-05-01 12.00AM
    """
    datetime_format = '%d-%m-%Y %H:%M%p'
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%d-%m-%Y')
    date_str = f"{yesterday} {time}"
    return datetime.strptime(date_str, datetime_format)

# TODO: Add unit tests
def get_polutant_unit(aq_value: str) -> tuple:
    """Get tuple(polutant, unit) from aq_value. Returns (None, None) if N/A"""
    if aq_value == "N/A":
        return (None, None, None)
    number = int(re.search(r'\d+', aq_value).group())
    text = re.sub(r'\d+', '', aq_value)
    
    match text:
        case "**":
            return ("PM2.5", "ug/m3", number)
        case "*":
            return ("PM10", "ug/m3", number)
        case "a":
            return ("S02", "ppm", number)
        case "b":
            return ("NO2", "ppm", number)
        case "c":
            return ("03", "ppm", number)
        case "d":
            return ("CO", "ppm", number)
        case "&":
            return ("multi-pollutant", None, number)
        case _:
            return ("unknown", None, number)
    
def download_from_gcs(bucket_path: str, save_path: str):
    gcp_cloud_storage_bucket_block: GcsBucket = GcsBucket.load("air-quality")
    gcp_cloud_storage_bucket_block.download_object_to_path(bucket_path, save_path)
    
if __name__ == "__main__":
    # values = ["64**", "64**", "63**", "62**", "62**", "61**", "61a", "60b", "5*", "59**", "58**", "57&", "56**", "56**", "56**", "55**", "56**", "55**", "55**", "N/A", "N/A", "N/A", "N/A"]
    # [print(get_polutant_unit(val)) for val in values]
    
    # download_from_gcs("daily_transformed_aq_data/18-03-2023.parquet", "18-03-2023.parquet")
    pass
    
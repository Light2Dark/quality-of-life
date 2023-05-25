IN_ORDER_TIMINGS = [
  "State",
  "Location",
  "1:00AM",
  "2:00AM",
  "3:00AM",
  "4:00AM",
  "5:00AM",
  "6:00AM",
  "7:00AM",
  "8:00AM",
  "9:00AM",
  "10:00AM",
  "11:00AM",
  "12:00PM",
  "1:00PM",
  "2:00PM",
  "3:00PM",
  "4:00PM",
  "5:00PM",
  "6:00PM",
  "7:00PM",
  "8:00PM",
  "9:00PM",
  "10:00PM",
  "11:00PM",
  "12:00AM"
]


def get_data_timings(response: dict) -> tuple:
    """Returns data and timings from response. If the key has changed, it will print the first key found and return it's value."""
    if "24hour_api_apims" in response:
        data = response["24hour_api_apims"]
    elif "24hour_api" in response:
        data = response["24hour_api"]
    else:
        key_list = list(response.keys())
        print(f"key has changed, selecting first key: {key_list[0]}")
        data = response[key_list[0]]
    timings = data[0]
    
    return data, timings



# For testing only, dbt takes over this code
def get_polutant_unit_value(aq_value: str) -> tuple:
    """Get tuple(polutant, unit, value) from aq_value. Returns (None, None, pd.NA) if N/A

    Args:
        aq_value (str): air quality value

    Returns:
        tuple(str, str, int | pd.NA): polutant, unit, value
    """
    import numpy as np
    import regex as re
    import pandas as pd
    
    if aq_value == "N/A":
        return (None, None, np.nan)
    text = re.sub(r'\d+', '', aq_value)
    number = re.search(r'\d+', aq_value).group()
    number = pd.to_numeric(number, errors="coerce")
    
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
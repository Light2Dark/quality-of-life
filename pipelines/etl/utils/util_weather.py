import os, requests, json
from dotenv import load_dotenv

load_dotenv()

# for station in WEATHER_STATIONS_LIST.keys():
#     url = f"https://api.weather.com/v1/location/{station}/observations/historical.json?apiKey={os.getenv('WEATHER_API')}&units=m&startDate=20230501&endDate=20230501"

#     response = requests.get(url)
#     print(response.status_code)
    
#     if response.status_code != 200:
#         print("Invalid station:", station)

import pandas as pd

df = pd.read_csv("airports.csv", header=0)

df.columns = df.columns.str.replace('\xa0', ' ')
df["City served"] = df["City served"].str.replace('\xa0', ' ')
df["State"] = df["State"].str.replace('\xa0', ' ')
df["Coordinates"] = df["Coordinates"].str.replace('\xa0', ' ')
df["Airport name"] = df["Airport name"].str.replace('\xa0', ' ')
df["IATA"] = df["IATA"].str.replace('\xa0', ' ')
df["ICAO"] = df["ICAO"].str.replace('\xa0', ' ')
df["Coordinates"] = df["Coordinates"].str.replace('\xa0', ' ')
df["Coordinates"] = df["Coordinates"].str.replace('\u2032', "'")

df.rename(columns={'City served': 'City', 'Airport name': 'Weather_station_name'}, inplace=True)

df["Weather_station_name"] = df["Weather_station_name"].apply(lambda x: x.split("/")[0].strip())
df["Weather_station_name"] = [s.rstrip('0123456789[]') for s in df["Weather_station_name"]]

from geopy.geocoders import ArcGIS
geolocator = ArcGIS()

df["Latitude"].dtype = "str"
df["Longitude"].dtype = "str"

df["Latitude"] = df["Coordinates"].apply(lambda x: geolocator.geocode(x).latitude)
df["Longitude"] = df["Coordinates"].apply(lambda x: geolocator.geocode(x).longitude)

df.to_csv("stations.csv", index=False)
print(df)
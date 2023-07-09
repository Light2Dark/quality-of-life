import os, requests, json
from dotenv import load_dotenv
import pandas as pd
from geopy.geocoders import Nominatim

load_dotenv()

def stations_csv():
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

def extract_personal_weather_stations():
    stations = [
        "ISELANGO11",
        "IPETAL1",
        "IPETAL3",
        "IKUALA16",
        "IKUALA6",
        "IKUALA23",
        "IAMPAN2",
        "IHULUL1",
        "IPUCHO2",
        "IPUCHO4",
        "IPUTRA2",
        "IKAJAN7",
        "IKAJAN12",
        "IKAJAN6",
        "IKAJAN4",
        "ISEMEN3",
        "INILAI8",
        "ITEMER1",
        "ITANJO2",
        "IRAUB2",
        "IBUKIT1",
        "IPENAN2",
        "IAYERI1",
        "IPENAN3",
        "IKUALA18",
        "IPADAN6",
        "IKUANT6",
        "IPEKAN1",
        "ISELAN1",
        "IMALAC2",
        "ITANGK1",
        "IPAGOH1",
        "IJOHORBA4"
    ]
    
    api_key = os.getenv("WEATHER_API_KEY")
    urls = [f"https://api.weather.com/v2/pws/observations/all/1day?apiKey={api_key}&stationId={station}&numericPrecision=decimal&format=json&units=m" for station in stations]
    
    for url in urls:
        response = requests.get(url)
        print(response.status_code)
        response_json = response.json()
        
        lat, long = response_json["observations"][0]["lat"], response_json["observations"][0]["lon"]
        station_id = response_json["observations"][0]["stationID"]
        
        geolocator = Nominatim(user_agent="weather_app")
        location = geolocator.reverse(f"{lat}, {long}")
        
        df = pd.DataFrame({
            "station_id": [station_id],
            "lat": [lat],
            "long": [long],
            "address": [location.address]
        })
        
        df.to_csv("pws.csv", mode="a", header=False, index=False)
        
def geocode():
    df = pd.read_csv("dbt/seeds/state_locations.csv")

    geolocator = Nominatim(user_agent="weather_app")
    
    def breakdown_address(row):
        raw_add = geolocator.reverse(f"{row['Latitude']}, {row['Longitude']}").raw['address']
        
        town = next((raw_add[key] for key in ['town', 'hamlet', 'county', 'suburb', 'neighbourhood', 'quarter', 'district'] if raw_add.get(key)), None)
        city = next((raw_add[key] for key in ['city', 'district', 'state_district', 'county', 'region', 'suburb'] if raw_add.get(key)), None)
        
        df = pd.DataFrame({
            "Identifying_Location": [row["Identifying_Location"]],
            "town": [town],
            "city": [city],
            "state": [raw_add.get('state')],
            "lat": [row["Latitude"]],
            "long": [row["Longitude"]],
            "postcode": [raw_add.get('postcode')],
            "address": [raw_add]
        })
        
        df.to_csv("new_sl.csv", mode="a", header=False, index=False)
        
    df.apply(breakdown_address, axis=1)
    
if __name__ == "__main__":
    # extract_personal_weather_stations()
    
    df = pd.read_csv("dbt/seeds/state_locations.csv", header=0)
    personal_weather_stations = df['PWStation'].dropna().unique()
    print(personal_weather_stations)
    
import os, requests, json, glob
from dotenv import load_dotenv
import pandas as pd
from geopy.geocoders import Nominatim
from google.cloud import bigquery
from datetime import datetime
from typing import List

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

def extract_personal_weather_stations(stations: List, save_path: str):
    api_key = os.getenv("WEATHER_API")
    urls = [f"https://api.weather.com/v2/pws/observations/all/1day?apiKey={api_key}&stationId={station}&numericPrecision=decimal&format=json&units=m" for station in stations]
    df = pd.DataFrame()
    
    for url in urls:
        response = requests.get(url)
        print(response.status_code)
        response_json = response.json()
        
        lat, long = response_json["observations"][0]["lat"], response_json["observations"][0]["lon"]
        station_id = response_json["observations"][0]["stationID"]
        
        geolocator = Nominatim(user_agent="weather_app")
        location = geolocator.reverse(f"{lat}, {long}")
        
        new_df = pd.DataFrame({
            "station_id": [station_id],
            "Latitude": [lat],
            "Longitude": [long],
            "address": [location.address]
        })
        df = pd.concat([df, new_df], axis=0, ignore_index=True)
        
    df.to_csv(save_path, index=False)
        
def geocode(locations_list_csv_path = "dbt/seeds/state_locations.csv", new_csv_path = "new_sl.csv"):
    locations_df = pd.read_csv(locations_list_csv_path)

    geolocator = Nominatim(user_agent="weather_app")
    new_df = pd.DataFrame()
    
    def breakdown_address(row):
        raw_add = geolocator.reverse(f"{row['Latitude']}, {row['Longitude']}").raw['address']
        
        town = next((raw_add[key] for key in ['town', 'hamlet', 'county', 'suburb', 'neighbourhood', 'quarter', 'district'] if raw_add.get(key)), None)
        city = next((raw_add[key] for key in ['city', 'district', 'state_district', 'county', 'region', 'suburb'] if raw_add.get(key)), None)
        
        df = pd.DataFrame({
            "Identifying_Location": [row.get("Identifying_Location")],
            "town": [town],
            "city": [city],
            "state": [raw_add.get('state')],
            "lat": [row["Latitude"]],
            "long": [row["Longitude"]],
            "postcode": [raw_add.get('postcode')],
            "address": [raw_add]
        })
        return df
    
    for index, row in locations_df.iterrows():
        df = breakdown_address(row)
        new_df = pd.concat([new_df, df], axis=0, ignore_index=True)
    
    new_df.to_csv(new_csv_path, index=False)
    
def make_pressure_df(file_str: str) -> pd.DataFrame:
    """
        Return a dataframe with pressure, datetime and weather station id.
        file_str (str): Used to limit the number of files to read.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google_creds.json'
    client = bigquery.Client()
    
    json_files = glob.glob(f"data/pws/{file_str}.json")
    
    weather_df = pd.DataFrame()
    
    for file in json_files:
        with open(file, "r") as f:
            print(f"Reading file... {file}")
            data = json.load(f)
            for key in data.keys():
                observations = data[key]["observations"]
                for obs in observations:
                    weather_station = str(obs['stationID'])
                    dtime = str(datetime.fromtimestamp(obs['epoch']))
                    
                    pressure_max = obs["metric"]["pressureMax"]
                    pressure_min = obs["metric"]["pressureMin"]
                    
                    if pressure_max and pressure_min:
                        pressure_avg = round((pressure_max + pressure_min) / 2,3)
                    elif pressure_max:
                        pressure_avg = pressure_max
                    elif pressure_min:
                        pressure_avg = pressure_min
                    else:
                        pressure_avg = None
                    
                    # dml_statement = (
                    #     f"""UPDATE `quality-of-life-364309.dev.hourly_pws_weather` SET pressure = CAST({pressure_avg} as STRING) WHERE datetime = '{dtime}' AND weather_station = '{weather_station}';"""
                    # )
                    # sql_statements.append(dml_statement)
                    
                    df = pd.DataFrame({
                        "datetime": [str(dtime)],
                        "weather_station": [str(weather_station)],
                        "pressure": [str(pressure_avg)]
                    })
                    weather_df = pd.concat([weather_df, df], axis=0, ignore_index=True)
            print(f"Finished reading file... {file}")
    
    weather_df = weather_df.astype(str)
    return weather_df
                    
    # sql_statements = "\n".join(sql_statements)
    # query_job = client.query(sql_statements)  # API request
    # query_job.result()  # Waits for statement to finish


def pressure_update():
    from pipelines.etl.utils.util_weather import make_pressure_df
    from pipelines.etl.load.upload import load_to_bq
    from typing import List
    import concurrent.futures

    def pws_pressure(date_str: str):
        df = make_pressure_df(date_str)
        load_to_bq.fn(df, "dev.hourly_pws_pressure", append=True)

    def mp_pws_pressure(date_list: List[str]):
        with concurrent.futures.ProcessPoolExecutor(7) as executor:
            executor.map(
                pws_pressure,
                [date for date in date_list]
            )

def stats():
    df = pd.read_csv("pws.csv")
    pws_stations = pd.read_csv("dbt/seeds/state_locations.csv")
    pws_stations = pws_stations['PWStation'].dropna().unique()
    
    def print_stats(df, factor: str):
        min_factor = df[factor].min()
        avg_factor = df[factor].mean()
        max_factor = df[factor].max()
        if avg_factor == 0:
            print("BRO")
        print(f"Station: {station}, Average {factor}: {avg_factor}, Max {factor}: {max_factor}, Min {factor}: {min_factor}")
    
    # print average uv reading for each station
    for station in pws_stations:
        df_station = df[df['weather_station'] == station]
        # avg_uv = df_station['uv'].mean()
        # max_uv = df_station['uv'].max()
        
        # print_stats(df_station, 'pressure')
        print_stats(df_station, 'uv')

        # print(f"Station: {station}, Average UV: {avg_uv}, Max UV: {max_uv}")

    
if __name__ == "__main__":
    # extract_personal_weather_stations()
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
    new_stations = [
        "IDENGK2",
        "INILAI4",
        "ISERIK8",
        "IKAJAN12",
        "IKUALA28",
        "ISHAHA4",
        "IBRINC1",
        "IGUAMU4",
        "IBANDA27",
        "ISUNGA4",
        "IKULAI5",
        "IKUCHI6",
        "IKUCHI10",
        "IKUCHI7",
        "ISIBU11",
        "INIAH2",
        "IPAPAR3",
        "ITAMPA3",
        "IKOTAB5",
        "IBELURAN3",
        "IBELURAN2"
    ]

    
    pass
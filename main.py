import argparse
from pipelines.config import PROD_DATASET_AQ, PROD_DATASET_WEATHER
from pipelines import air_quality, weather
from infra import prefect_infra

def run_aq_pipeline():
    air_quality.elt_flow(date_start="2021-05-11", date_end="2021-05-11", time="0000", dataset="dev.hourly_air_quality")

def run_aq_parser():
    parser = argparse.ArgumentParser(prog="Air Quality ELT", description="An ELT flow to get air quality data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", type=bool,help="If true, dev dataset is use. Else, prod dataset", required=True)
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=True)
    args = parser.parse_args()

    if args.testing:
        air_quality.elt_flow(args.start_date, args.end_date, args.time)
    elif args.testing is False:
        air_quality.elt_flow(args.start_date, args.end_date, args.time, PROD_DATASET_AQ)
        
def run_weather_parser():
    parser = argparse.ArgumentParser(prog="Weather ELT", description="An ELT flow to get weather data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", type=bool,help="If true, dev dataset is use. Else, prod dataset", required=True)
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYYMMDD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYYMMDD.", required=True)
    args = parser.parse_args()
    
    if args.testing:
        weather.elt_weather(args.start_date, args.end_date)
    elif args.testing is False:
        weather.elt_weather(args.start_date, args.end_date, PROD_DATASET_WEATHER)

if __name__ == "__main__":
    
    # lets do 2017 - 2017 first
    # weather.elt_weather("20170101", "20171231", PROD_DATASET_WEATHER)
    
    # run_weather_parser()
    # prefect_infra.create_deployment()
    
    # 2000 - 2002
    # weather.elt_weather("20000101", "20021231", PROD_DATASET_WEATHER)
    
    # 1999
    # weather.elt_weather("19990101", "19991231", PROD_DATASET_WEATHER)
    
    # 1995 - 1998
    # weather.elt_weather("19950101", "19981231", PROD_DATASET_WEATHER)
    
    # 2003 - 2010
    # weather.elt_weather("20100722", "20101231", PROD_DATASET_WEATHER)
    
    air_quality.elt_flow("2023-01-01", "2023-01-31", "0000")
    
    pass
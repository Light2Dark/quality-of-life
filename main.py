import argparse
from pipelines.config import PROD_DATASET_AQ, PROD_DATASET_WEATHER
from pipelines import air_quality, weather, historical_aq
from infra import prefect_infra

def run_aq_pipeline():
    air_quality.elt_flow(date_start="2021-05-11", date_end="2021-05-11", time="0000", dataset="dev.hourly_air_quality")
        
def run_full_weather_parser():
    parser = argparse.ArgumentParser(prog="Full Weather ELT", description="An ELT flow to get weather and air quality data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", type=bool,help="If true, dev dataset is use. Else, prod dataset", required=True)
    parser.add_argument("-aq", "--air_quality", type=bool, help="If true, air quality data is requested from API and stored in GCS & BQ", default=True)
    parser.add_argument("-w", "--weather", type=bool, help="If true, weather data is requested from API and stored in GCS & BQ", default=True)
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYYMMDD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYYMMDD.", required=True) 
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=False, default="0000")
    args = parser.parse_args()
    
    # convert YYYYMMDD to YYYY-MM-DD
    aq_start_date = args.start_date[:4] + "-" + args.start_date[4:6] + "-" + args.start_date[6:]
    aq_end_date = args.end_date[:4] + "-" + args.end_date[4:6] + "-" + args.end_date[6:]
    
    if args.air_quality:
        if args.testing:
            air_quality.elt_flow(aq_start_date, aq_end_date, args.time)
        else:
            air_quality.elt_flow(aq_start_date, aq_end_date, args.time, PROD_DATASET_AQ)
    
    if args.weather:
        if args.testing:
            weather.elt_weather(args.start_date, args.end_date)
        else:
            weather.elt_weather(args.start_date, args.end_date, PROD_DATASET_WEATHER)

if __name__ == "__main__":
    ## Running the elt_historical_air_quality_pipeline
    # historical_aq.elt_archive("dev.historic_air_quality")
    
    prefect_infra.create_deployment()  ## Run this only once to create prefect blocks
    
    # main flow
    run_full_weather_parser()
    
    pass
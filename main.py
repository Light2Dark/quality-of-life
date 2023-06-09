import argparse
from pipelines import air_quality, weather, historical_aq
from infra import prefect_infra

PROD_DATASET_WEATHER = "prod.hourly_weather"
DEV_DATASET_WEATHER = "dev.hourly_weather"
RAW_WEATHER_DATA_GCS_SAVEPATH = "hourly_weather_data"
PREPROCESSED_WEATHER_DATA_GCS_SAVEPATH = "preprocessed_weather_data"

PROD_DATASET_AQ = "prod.hourly_air_quality"
DEV_DATASET_AQ = "dev.hourly_air_quality"
RAW_AQ_DATA_GCS_SAVEPATH = "daily_aq_data"
PREPROCESSED_AQ_DATA_GCS_SAVEPATH = "daily_preprocessed_air_quality_data"


def run_aq_pipeline():
    air_quality.elt_flow(date_start="2021-05-11", date_end="2021-05-11", time="0000", dataset="dev.hourly_air_quality")
        
def run_full_weather_parser():
    parser = argparse.ArgumentParser(prog="Full Weather ELT", description="An ELT flow to get weather and air quality data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", action="store_true",help="If true, dev dataset is used. Else, prod dataset")
    parser.add_argument("-aq", "--air_quality", action="store_true", help="If true, air quality data is requested from API and stored in GCS & BQ")
    parser.add_argument("-w", "--weather", action="store_true", help="If true, weather data is requested from API and stored in GCS & BQ")
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYYMMDD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYYMMDD.", required=True) 
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=False, default="0000")
    args = parser.parse_args()
    
    # convert YYYYMMDD to YYYY-MM-DD
    aq_start_date = args.start_date[:4] + "-" + args.start_date[4:6] + "-" + args.start_date[6:]
    aq_end_date = args.end_date[:4] + "-" + args.end_date[4:6] + "-" + args.end_date[6:]
    
    if args.air_quality:
        if args.testing:
            air_quality.elt_flow(RAW_AQ_DATA_GCS_SAVEPATH, PREPROCESSED_AQ_DATA_GCS_SAVEPATH, DEV_DATASET_AQ, aq_start_date, aq_end_date, args.time)
        else:
            air_quality.elt_flow(RAW_AQ_DATA_GCS_SAVEPATH, PREPROCESSED_AQ_DATA_GCS_SAVEPATH, PROD_DATASET_AQ, aq_start_date, aq_end_date, args.time)
    
    if args.weather:
        if args.testing:
            weather.elt_weather(RAW_WEATHER_DATA_GCS_SAVEPATH, PREPROCESSED_WEATHER_DATA_GCS_SAVEPATH, DEV_DATASET_WEATHER, args.start_date, args.end_date)
        else:
            weather.elt_weather(RAW_WEATHER_DATA_GCS_SAVEPATH, PREPROCESSED_WEATHER_DATA_GCS_SAVEPATH, PROD_DATASET_WEATHER, args.start_date, args.end_date)
            
            
def benchmark_multiprocessing():
    import multiprocessing as mp
    import time
    
    start_time = time.perf_counter()
    weather.elt_weather("test_hourly_weather", "test_preprocessed_weather", "test.hourly_weather", "20210101", "20210331") # Running 3 months
    end_time = time.perf_counter()
    with open("benchmark.txt", "w") as f:
        f.write(f"Time taken without multiprocessing: {end_time - start_time}\n")
    
    
    p1 = mp.Process(target=weather.elt_weather, args=("test_hourly_weather", "test_preprocessed_weather", "test.hourly_weather", "20210101", "20210131")).start()
    p2 = mp.Process(target=weather.elt_weather, args=("test_hourly_weather", "test_preprocessed_weather", "test.hourly_weather", "20210201", "20210228")).start()
    p3 = mp.Process(target=weather.elt_weather, args=("test_hourly_weather", "test_preprocessed_weather", "test.hourly_weather", "20210301", "20210331")).start()
    
    processes = [p1, p2, p3]
    start_time = time.perf_counter()
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    end_time = time.perf_counter()
    with open("benchmark.txt", "a") as f:
        f.write(f"Time taken with multiprocessing: {end_time - start_time}")
            

if __name__ == "__main__":
    ## Running the elt_historical_air_quality_pipeline
    # historical_aq.elt_archive("dev.historic_air_quality")
    
    # prefect_infra.create_deployment()  ## Run this only once to create prefect blocks
    
    # main flow
    # run_full_weather_parser()
    
    benchmark_multiprocessing()
    
    pass
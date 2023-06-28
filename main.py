import argparse, concurrent.futures, pytz
import multiprocessing as mp
from pipelines import air_quality, weather, historical_aq
from datetime import datetime, timedelta
from typing import List, Tuple
from infra import prefect_infra
from pipelines.config import timeit

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
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYYMMDD. Defaults to today")
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYYMMDD. Defaults to today") 
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=False, default="0000")
    parser.add_argument("-p", "--parallel", type=int, help="Number of parallel processes to run. Defaults to cpu count - 1", required=False, default=mp.cpu_count() - 1)
    args = parser.parse_args()
    
    # sets num of processes to run in parallel concurrently
    num_processes = args.parallel // 2 if args.air_quality and args.weather else args.parallel
    print(f"Number of processes available: {num_processes}")
    
    if args.air_quality:
        if args.start_date is None or args.end_date is None:
            print("Start date or end date not specified, using default of today")
            aq_start_date = get_datetime_today('%Y-%m-%d')
            aq_end_date = get_datetime_today('%Y-%m-%d')
        else:        
            # convert YYYYMMDD to YYYY-MM-DD for aq pipeline
            aq_start_date = args.start_date[:4] + "-" + args.start_date[4:6] + "-" + args.start_date[6:]
            aq_end_date = args.end_date[:4] + "-" + args.end_date[4:6] + "-" + args.end_date[6:]
        
        if args.testing:
            print("Running air quality pipeline on dev dataset")
            air_quality_multiprocessing(aq_start_date, aq_end_date, RAW_AQ_DATA_GCS_SAVEPATH, PREPROCESSED_AQ_DATA_GCS_SAVEPATH, DEV_DATASET_AQ, num_processes, args.time.strip())
        else:
            air_quality_multiprocessing(aq_start_date, aq_end_date, RAW_AQ_DATA_GCS_SAVEPATH, PREPROCESSED_AQ_DATA_GCS_SAVEPATH, PROD_DATASET_AQ, num_processes, args.time.strip())
    
    if args.weather:
        if args.start_date is None or args.end_date is None:
            print("Start date or end date not specified, using default of today")
        start_date = get_datetime_today('%Y%m%d') if args.start_date is None else args.start_date.strip()
        end_date = get_datetime_today('%Y%m%d') if args.end_date is None else args.end_date.strip()
        
        if args.testing:
            print("Running weather pipeline on dev dataset")
            weather_multiprocessing(start_date, end_date, RAW_WEATHER_DATA_GCS_SAVEPATH, PREPROCESSED_WEATHER_DATA_GCS_SAVEPATH, DEV_DATASET_WEATHER, num_processes)
        else:
            print("Running weather pipeline on prod dataset")
            weather_multiprocessing(start_date, end_date, RAW_WEATHER_DATA_GCS_SAVEPATH, PREPROCESSED_WEATHER_DATA_GCS_SAVEPATH, PROD_DATASET_WEATHER, num_processes)
         
         
def get_datetime_today(format_date: str) -> str:
    """Returns datetime in the format specified in format_date. By default, returns today's date
    """
    return datetime.now(tz=pytz.timezone('Asia/Kuala_Lumpur')).strftime(format_date)
            
def pickled_weather(*args, **kwargs):
    # allows multiprocessing to work with weather.elt_weather
    weather.elt_weather(*args, **kwargs)
    
def pickled_aq(*args, **kwargs):
    # allows multiprocessing to work with air_quality.elt_flow
    air_quality.elt_air_quality(*args, **kwargs)


# @timeit
def weather_multiprocessing(start_date: str, end_date: str, raw_gcs_savepath: str, preproc_gcs_savepath: str, dataset: str, num_processes: int):
    # Run weather ELT in parallel
    # Execute the max number of processes concurrently until all date chunks are processed
    # TODO: Unittest this
    start_datetime = datetime.strptime(start_date, "%Y%m%d")
    end_datetime = datetime.strptime(end_date, "%Y%m%d")
    date_chunks = get_date_chunks(start_datetime, end_datetime, 30, "%Y%m%d") # 30 days per chunk is the max allowed by the API
    
    if num_processes >= len(date_chunks):
        print("Running weather ELT in 1 run")
        print(date_chunks)
        run_weather_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks, len(date_chunks))
    else:
        print(f"Total date chunks: {len(date_chunks)}")
        remaining_chunks = len(date_chunks) % num_processes
        if remaining_chunks > 0:
            print(f"Odd number of date chunks. Running remainder chunks: 0-{remaining_chunks}")
            print(date_chunks[:remaining_chunks])
            run_weather_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks[:remaining_chunks], remaining_chunks)
        
        for i in range(remaining_chunks, len(date_chunks), num_processes):
            print(f"Running date chunks {i}-{i+num_processes}")
            print(date_chunks[i:i+num_processes])
            run_weather_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks[i:i+num_processes], num_processes)
        

def run_weather_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks, num_processes):
    with concurrent.futures.ProcessPoolExecutor(num_processes) as executor:
        executor.map(
            pickled_weather, 
            [raw_gcs_savepath] * num_processes, 
            [preproc_gcs_savepath] * num_processes, 
            [dataset] * num_processes, 
            [date[0] for date in date_chunks], 
            [date[1] for date in date_chunks]
        )
        
def run_aq_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks, time, num_processes):
    # print(f"Number of processes: {num_processes}, Number of date chunks: {len(date_chunks)}")
    with concurrent.futures.ProcessPoolExecutor(num_processes) as executor:
        executor.map(
            pickled_aq,
            [raw_gcs_savepath] * num_processes,
            [preproc_gcs_savepath] * num_processes,
            [dataset] * num_processes,
            [date[0] for date in date_chunks],
            [date[1] for date in date_chunks],
            [time] * num_processes
        )

def air_quality_multiprocessing(start_date: str, end_date: str, raw_gcs_savepath: str, preproc_gcs_savepath: str, dataset: str, num_processes: int, time: str):
    # Run air quality ELT in parallel
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    
    # break up start and end date into chunks of 1 day
    date_chunks = get_date_chunks(start_datetime, end_datetime, 0, "%Y-%m-%d")
    
    if num_processes >= len(date_chunks):
        print("Running air quality ELT in 1 run")
        print(date_chunks)
        run_aq_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks, time, len(date_chunks))
    else:
        print(f"Total date chunks: {len(date_chunks)}")
        remaining_chunks = len(date_chunks) % num_processes
        if remaining_chunks > 0:
            print(f"Odd number of date chunks. Running remainder chunks: 0-{remaining_chunks}")
            print(date_chunks[:remaining_chunks])
            run_aq_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks[:remaining_chunks], time, remaining_chunks)
        
        for i in range(remaining_chunks, len(date_chunks), num_processes):
            print(f"Running date chunks {i}-{i+num_processes}")
            print(date_chunks[i:i+num_processes])
            run_aq_flows(raw_gcs_savepath, preproc_gcs_savepath, dataset, date_chunks[i:i+num_processes], time, num_processes)
    

    
def get_date_chunks(start_datetime: datetime, end_datetime: datetime, chunksize: int, date_format: str) -> List[Tuple[str, str]]:
    """Extracts weather data in <chunksize day chunks as that is the max number of days allowed by the API

    Args:
        start_date (datetime): Start date in datetime format
        end_date (datetime): End date in datetime format

    Returns:
        List(Tuple(str, str)): Chunks of start and end dates in YYYYMMDD format. Eg: [("20200101", "20200131"), ("20200201", "20200229"))]
    """
    
    temp_start_date = start_datetime
    temp_end_date = start_datetime

    chunks = []
    while temp_start_date <= end_datetime:
        temp_end_date = temp_start_date + timedelta(days=chunksize)
        if temp_end_date > end_datetime:
            temp_end_date = end_datetime

        chunks.append((temp_start_date.strftime(date_format), temp_end_date.strftime(date_format)))
        
        temp_start_date = temp_end_date + timedelta(days=1)

    return chunks

if __name__ == "__main__":
    ## Running the elt_historical_air_quality_pipeline
    # historical_aq.elt_archive("dev.historic_air_quality")
    
    # prefect_infra.create_deployment()  ## Run this only once to create prefect blocks
    
    # main flow
    run_full_weather_parser() 
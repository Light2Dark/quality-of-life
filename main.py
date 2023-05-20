import argparse
from pipelines.config import PROD_DATASET
from pipelines.air_quality import elt_flow
from infra import prefect_infra

def run_aq_pipeline():
    elt_flow(date_start="2021-05-11", date_end="2021-05-11", time="0000", dataset="dev.hourly_air_quality")

def run_parser():
    parser = argparse.ArgumentParser(prog="Air Quality ELT", description="An ELT flow to get air quality data from API and store in GCS & BQ", epilog="credits to Sham")
    parser.add_argument("-t", "--testing", type=bool,help="If true, dev dataset is use. Else, prod dataset", required=True)
    parser.add_argument("-sd", "--start_date", type=str, help="Start date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-ed", "--end_date", type=str, help="End date to request data from API. Format is YYYY-MM-DD.", required=True)
    parser.add_argument("-tm", "--time", type=str, help="Time to request data from API. Format is HHMM. Defaults to 12.00am", required=True)
    args = parser.parse_args()

    if args.testing:
        elt_flow(args.start_date, args.end_date, args.time)
    elif args.testing is False:
        elt_flow(args.start_date, args.end_date, args.time, PROD_DATASET)

if __name__ == "__main__":
    run_parser()
    pass
# Setup instructions



## GitHub Setup
1. Set .env variables:
- PREFECT_API_URL
- PRFECT
...


2. In your terminal, from the root folder of this project, run `bash setup_infra.sh`

You are ready to go to run the main elt pipeline.
3. Run `python main.py --testing=True --start_date=2020-01-01 --end_date=2020-01-02 --time=0000`

Run dbt
4. cd into dbt folder
5. Run `dbt init`
...
# Malaysia Air Quality Data Pipeline

Air quality data is collected from air quality stations and updated to a government API. This API is updated every hour but there is no public database to hold all that information for long-term analysis. Furthermore, existing datasets do not store information like pollutant types. This project aims to create an automated end-to-end data pipeline and a dashboard on the available data.

<p align="center">
  <img src="https://user-images.githubusercontent.com/19585239/233954611-cf04cf0b-36cd-4c5d-a3c7-e1fb9d2a90fc.png" height="350px"/>
</p>

**Questions that can be answered:**
- What is the trend of air quality in Malaysia? 
- Which are the best places to live in terms of air quality?
- Are there any patterns in air quality according to the time of day?

## Features
- Dashboard: [Looker Studio Report](https://lookerstudio.google.com/reporting/42328c2a-5493-4dfa-9cb6-54b747f4f69a)
- Air Quality Data from 2017-Present
- Fresh daily data, queryable. Refer to [BigQuery Section](#bigquery)
- Daily workflow that can be observed through Prefect Cloud. Alerts whenever a workflow has failed

## Architecture
![image](https://github.com/Light2Dark/quality-of-life/assets/19585239/81aa6dc0-75bd-44a1-b7a0-79978f4e54b1)

- **Orchestration:** GitHub Actions, Prefect Cloud, dbt Cloud
- **Infrastructure:** GitHub Actions, GCP
- **Data Extraction:** Python
- **Data Transformation & Load:** dbt, Pandas
- **Data Warehouse:** BigQuery
- **Data Visualization:** Google Looker Studio

### Data Transformation:
<img src="https://user-images.githubusercontent.com/19585239/233899390-7901932b-d2e1-4290-ad27-aca83758c8f8.png" width="80%" />
Pandas is used to clean data while dbt is used for heavy processing like mapping, joins and running tests on data sources.

### Schema
<img src="https://github.com/Light2Dark/quality-of-life/assets/19585239/2ff0d122-788b-462b-8748-3c21cfdb3c84" height="400px" />

**Clustering:** These tables do not need clustering as [recommended here](https://cloud.google.com/bigquery/docs/clustered-tables) due to the table size <1GB

**Partitioning:** While it would be more efficient to partition by states, BigQuery does not allow partitions by String fields. Some workarounds exist (adding an int as an additional column) but the benefits do not outweigh the cons of this approach. Namely, our queries would need to be modified to use this column.

### Data Sources
<p float="left">
  <img width = "550px" src = "https://user-images.githubusercontent.com/19585239/195292149-ac7e48d1-8d98-4b85-9533-8616aca9a58d.png" />
  <img height = "300px" src = "https://user-images.githubusercontent.com/19585239/195292738-30a6ae22-a266-4456-9634-fc5ee7217ebc.png" />
</p>

[APIMS Table](http://apims.doe.gov.my/api_table.html) 

### Dashboard
Access the dashboard here: [Looker Studio Report](https://lookerstudio.google.com/reporting/42328c2a-5493-4dfa-9cb6-54b747f4f69a)

You may notice the heatmap at the bottom shows flat colours, that's because the differences between columns are small, however this is something that needs to be improved on (if you know how, let me know!)

<img src="https://user-images.githubusercontent.com/19585239/234033506-2dbb9e36-1f4c-4d1c-ae8a-1c1b4b03c27a.png" height="400px" />

## Analyzing the data

You can analyze the data in 2 ways.
1. csv file `data/air_quality_2017-2023.zip`. This file is updated to 29/5/2023.
2. BigQuery.

This project uses BigQuery as a Data Warehouse, so you can use SQL to query data. All the tables in the prod dataset is public.

`PROJECT_ID=quality-of-life-364309`

`DATASET=prod`

There are several tables in the `prod` dataset, you may want to use the `air_quality` and `air_quality_full` tables for queries.

Example SQL Statements

```bash
  SELECT *
  FROM `quality-of-life-364309.prod.air_quality`
  LIMIT 1000
```

You can play around with BigQuery SQL using Kaggle. A sample notebook: [Shahmir's AQ Kaggle Notebook](https://www.kaggle.com/datasets/shahmirvarqha/air-quality-malaysia-2017-present)

### Additional Features 
**Tests:** Some transformation is done in Python and dbt. Several tests are done after running to ensure the data processed is as intended.

**GitHub Actions:** Before merging into main, a CI/CD pipeline checks to see if the unittests work.


## Installation

Python 3 is required for this project. Additionally, the entire project runs daily on the Cloud. Thus, the following accounts are needed:
- [Google Cloud Account](https://console.cloud.google.com/)
- [Prefect Cloud Account](https://app.prefect.cloud/)
- [dbt Cloud](https://cloud.getdbt.com/)

1. Setup your environment
```bash
  git clone <url>
  cd <project-name>

  python -m venv venv     # create a virtual environment
  source venv/bin/activate    # activate the virtual environment

  pip install -r requirements.txt   # installing dependencies
```
2. Download the service_account_json_file from GCP. Follow [service_account_file_download](https://github.com/wjuszczyk/dezoomcamp-project#step-2-setup-gcp)
3. Fill in the `.env.example` file and rename it to `.env`. Do not remove the # symbols!
```.env
## Prefect Config
PREFECT_API_ACCOUNT_ID=<PREFECT_API_ACCOUNT_ID>
PREFECT_API_WORKSPACE_ID=<PREFECT_API_WORKSPACE_ID>
PREFECT_API_KEY=<PREFECT_API_KEY>
#
## Prefect Blocks
# GitHub Blocks (Optional)
GITHUB_REPO=<GITHUB_REPO>
GITHUB_BRANCH=<GITHUB_BRANCH>
GITHUB_BLOCK=<GITHUB_BLOCK>
#
## GCP Config
PROJECT_ID=<PROJECT_ID>
REGION=<REGION>
GCP_CREDENTIALS_FILEPATH=<GCP_CREDENTIALS_FILEPATH>
#
## GitHub Email Config (Work in Progress)
SMTP=<smtp+starttls://user:password@server:port>
#
## Temperature API (Work in Progress)
ACCU_WEATHER_API_KEY=<ACCU_WEATHER_API_KEY>
```
4. Setup the infrastructure
In your terminal, from the root folder of this project, run the following command
```bash
# This will create the GCP resources (buckets + bigquery dataset), create the prefect connection and blocks.
bash setup_infra.sh
```

5. You are ready to run the main elt pipeline. Run the following command to extract air quality data from 2020-01-01 to 2020-01-02
```python
python main.py --testing=True --start_date=2020-01-01 --end_date=2020-01-02 --time=0000
```

6. Setup dbt. Firstly, modify the `dbt/profile_template.yml` file with your own project details.
```dbt/profile_template.yml
fixed:
  dataset: dev
  job_execution_timeout_seconds: 300
  job_retries: 1
  keyfile: <PATH_TO_GCP_CREDENTIALS_JSON_FILE>
  location: <REGION>
  method: service-account
  priority: interactive
  project: <PROJECT_ID>
  type: bigquery
prompts:
  user:
    type: string
    hint: yourname@jaffleshop.com
  threads:
    hint: "number of threads"
    type: int
    default: 4
```

5. Run dbt.
```bash
cd dbt
dbt init    # answer the prompts
dbt deps
dbt seed
dbt run
```


## Contributing

Contributions are always welcome!

#### Improvements (To-Do):

- Add logos for the sources of data in dashboard (APIMS)
- It might be good to partition and cluster based on certain attributes to provide long term scalability
- Add historical air quality data ([Hong Lim's Kaggle Dataset](https://www.kaggle.com/datasets/honglim/malaysia-air-quality-index-2017), [YnShung's API Malaysia](https://github.com/ynshung/api-malaysia))
- Dashboard's time_of_day breakdown can be a better heatmap that changes colour based on running median instead of fixed scale.

## Credits
Thank you to everyone who made the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

<img src="https://user-images.githubusercontent.com/19585239/234007526-9b07c079-70f0-4f8e-985b-03b7ad6b9dc7.png" width="500px" />


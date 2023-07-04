# Malaysia Weather Data Pipeline

Weather data is collected from weather stations throughout Malaysia. Government or third party providers collect this data. Unfortunately, these datasets are not publicly accessible and are error-prone. This project aims to create an automated end-to-end data pipeline and a dashboard on Malaysian weather.

<p align="center">
  <img src="https://user-images.githubusercontent.com/19585239/233954611-cf04cf0b-36cd-4c5d-a3c7-e1fb9d2a90fc.png" height="500px"/>
</p>

**Questions that can be answered:**
- How has our environment changed over the past 10 years?
- Which are the best places to live in terms of weather?
- Are there any patterns in air quality, temperature and humidity according to the time of day?

## Features
- Dashboard: [Looker Studio Report](https://lookerstudio.google.com/reporting/f0bac475-b860-4d01-8ac0-7dceae960daf)
- Weather data from 1996-Present*
- Fresh daily data, publicly available. Refer to [analyzing the data](#analyzing-the-data)
- Daily workflow that can be observed through Prefect Cloud. GitHub Actions, Prefect & dbt sends an email if the workflow has failed at any stage.

## Architecture
![image](https://github.com/Light2Dark/quality-of-life/assets/19585239/19ec5326-2350-44cb-b972-bba0a86f614c)

- **Orchestration:** GitHub Actions, Prefect Cloud, dbt Cloud
- **Infrastructure:** GitHub Actions, GCP, Terraform
- **Data Extraction:** Python
- **Data Transformation & Load:** dbt, Pandas
- **Data Lake & Warehouse:** Google Cloud Storage, BigQuery
- **Data Visualization:** Google Looker Studio

### Data Transformation:
<img src="https://github.com/Light2Dark/quality-of-life/assets/19585239/b56672ad-9957-44c8-a131-dbb4840b4e3a" width="80%" />

Both pandas and dbt is used to clean, transform and model the data

### Schema
![image](https://github.com/Light2Dark/quality-of-life/assets/19585239/1a95389e-9c5e-4aa6-8a8d-c6945aef1b1c)

**Clustering:** TODO [recommended here](https://cloud.google.com/bigquery/docs/clustered-tables).

**Partitioning:** It may be more efficient to partition by states however BigQuery does not allow partitions by String fields. Some workarounds exist (adding an int as an additional column) but the pros and cons needs to be examined.

**Normalization:** TODO. There are duplicated city fields, changing them would require reconnecting the dashboard which is a pain.

### Data Sources
The Weather data is proprietary and unfortunately this code is not reproducible without the API key. Credits to [Weather Underground](https://www.wunderground.com/) for the data. Contact me for more details regarding this.
<p float="left">
  <img width = "650px" src = "https://github.com/Light2Dark/quality-of-life/assets/19585239/283ec5a9-93c4-4bb2-87c8-c04fb9703bb9" />
  <img height = "80px" src = "https://github.com/Light2Dark/quality-of-life/assets/19585239/2c24acc2-08a1-4813-b3c1-46b620ed393a" />
</p>

The air quality data is extracted from the government website [APIMS Table](http://apims.doe.gov.my/api_table.html).
<p float="left">
  <img width = "600px" src = "https://user-images.githubusercontent.com/19585239/195292149-ac7e48d1-8d98-4b85-9533-8616aca9a58d.png" />
  <img height = "300px" src = "https://user-images.githubusercontent.com/19585239/195292738-30a6ae22-a266-4456-9634-fc5ee7217ebc.png" />
</p>

### Dashboard
Access the dashboard here: [Looker Studio Report](https://lookerstudio.google.com/reporting/f0bac475-b860-4d01-8ac0-7dceae960daf)

<img src="https://github.com/Light2Dark/quality-of-life/assets/19585239/4183283e-c191-4d46-9672-85187fcda9f9" height="400px" />


## Analyzing the data

You can analyze the data in 2 ways.
1. csv file `data/air_quality_2017-2023.zip`. This file is updated to 29/5/2023.
2. BigQuery.

This project uses BigQuery as a Data Warehouse, so you can use SQL to query data. All the tables in the prod dataset is public.

`PROJECT_ID=quality-of-life-364309`

`DATASET=prod`

There are several tables in the `prod` dataset, you may want to use the `full_weather`, `air_quality`, `weather` and `uv` tables for queries.

Example SQL Statements

```bash
  SELECT *
  FROM `quality-of-life-364309.prod.full_weather`
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
#
## GCP Config
PROJECT_ID=<PROJECT_ID>
REGION=<REGION>
GCP_CREDENTIALS_FILEPATH=<GCP_CREDENTIALS_FILEPATH>
#
## Weather API
WEATHER_API=<WEATHER_API_KEY>
#
## GitHub Email Config (Work in Progress)
SMTP=<smtp+starttls://user:password@server:port>
# GitHub Blocks (Optional)
GITHUB_REPO=<GITHUB_REPO>
GITHUB_BRANCH=<GITHUB_BRANCH>
GITHUB_BLOCK=<GITHUB_BLOCK>
#
```
4. Setup the infrastructure
In your terminal, from the root folder of this project, run the following command
```bash
# This will create the GCP resources (buckets + bigquery dataset), create the prefect connection and blocks.
bash setup_infra.sh
```

5. You are ready to run the main elt pipeline. Run the following command to extract air quality and weather data from 2020-01-01 to 2020-01-02 using 1 process only.
```python
python main.py \
  --testing=True \
  --air_quality=True \
  --weather=True \
  --start_date=20200101 \
  --end_date=20200102 \
  --time=0000 \
  --parallel=1
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

- It might be good to partition and cluster based on certain attributes to provide long term scalability
- Queries are slow, SQL used in dbt models may deserve improvement

## Credits
Thank you to everyone who made the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

<img src="https://user-images.githubusercontent.com/19585239/234007526-9b07c079-70f0-4f8e-985b-03b7ad6b9dc7.png" width="500px" />


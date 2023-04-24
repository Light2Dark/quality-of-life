# Malaysia Air Quality Data Pipeline

Air quality data is collected from air quality stations and updated to a government API. This API is updated every hour but there is no public database to hold all that information for long-term analysis. This project aims to create an end-to-end data pipeline and a dashboard on the available data.

<p align="center">
  <img src="https://user-images.githubusercontent.com/19585239/233954611-cf04cf0b-36cd-4c5d-a3c7-e1fb9d2a90fc.png" height="350px"/>
</p>

**Questions that can be answered:**
- What is the trend of air quality in Malaysia? 
- Which are the best places to live in terms of air quality?
- Are there any patterns in air quality according to the time of day?

## Features
- Dashboard: [Looker Studio Report](https://lookerstudio.google.com/reporting/42328c2a-5493-4dfa-9cb6-54b747f4f69a)
- Air Quality Data from 2017-Present*
- Fresh daily data, queryable. Refer to [BigQuery Section](#bigquery)
- Daily workflow that can be observed through Prefect Cloud. Alerts whenever a workflow has failed

## Architecture
![image](https://user-images.githubusercontent.com/19585239/233957402-be416dee-cfee-42ff-bbf5-3458a73b0990.png)

- **Orchestration:** GitHub Actions, Prefect Cloud, dbt Cloud
- **Infrastructure:** GitHub Actions, GCP
- **Data Warehouse:** BigQuery
- **Data Transformation:** dbt, Pandas
- **Data Visualization:** Google Looker Studio

### Data Transformation:
<img src="https://user-images.githubusercontent.com/19585239/233899390-7901932b-d2e1-4290-ad27-aca83758c8f8.png" width="80%" />
Pandas is used to clean data while dbt is used for heavy processing like mapping, joins and running tests on data sources.

### Schema
<img src="https://user-images.githubusercontent.com/19585239/233950343-f51347f3-8c5f-4cae-86d7-7c202f6e391f.png" height="400px" />

### Data Sources
<p float="left">
  <img width = "650px" src = "https://user-images.githubusercontent.com/19585239/195292149-ac7e48d1-8d98-4b85-9533-8616aca9a58d.png" />
  <img height = "350px" src = "https://user-images.githubusercontent.com/19585239/195292738-30a6ae22-a266-4456-9634-fc5ee7217ebc.png" />
</p>

[APIMS Table](http://apims.doe.gov.my/api_table.html) 

## Installation

Python 3 is required for this project. Additionally, the entire project runs daily on the Cloud. Thus, the following is needed:
- Prefect Cloud Authentication for GitHub Actions (`PREFECT_API_KEY` and `PREFECT_API_URL` environment variables)
- GCS and BigQuery connection in Prefect
- dbt Cloud

```bash
  git clone <url>
  cd <project-name>

  python -m venv venv     # create a virtual environment
  source venv/bin/activate    # activate the virtual environment

  pip install -r requirements.txt   # installing dependencies

  python flows/elt_web_to_bq.py    # runs the main code of this project.
```

## BigQuery

This project uses BigQuery as a Data Warehouse, giving you the power to use SQL to query data. All the tables in the prod dataset is public.

`PROJECT_ID=quality-of-life-364309`

`DATASET=prod`

There are several tables in the `prod` dataset, you may want to use the `air_quality` and `air_quality_full` tables for queries.

Example SQL Statements

```bash
  SELECT *
  FROM `quality-of-life-364309.prod.air_quality`
  LIMIT 1000
```

You can play around with BigQuery SQL using Kaggle

[Kaggle](https://www.kaggle.com/code/dansbecker/getting-started-with-sql-and-bigquery)

### Additional Features 
Tests: Some transformation is done in Python and dbt. Several tests are done after running to ensure the data processed is as intended.
GitHub Actions: Before merging into main, a CI/CD pipeline checks to see if the unittests work.

## Contributing

Contributions are always welcome!

#### Improvements:

*Add historical air quality data ([Hong Lim's Kaggle Dataset](https://www.kaggle.com/datasets/honglim/malaysia-air-quality-index-2017), [YnShung's API Malaysia](https://github.com/ynshung/api-malaysia))

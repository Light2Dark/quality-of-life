# Malaysia Air Quality Data Pipeline

Air quality data is provided on a government site in Malaysia. This API is updated every hour, unfortunately, the dataset for this hourly data is not recorded. A lot of information for environmental & air pollution analysis is lost.

This project aims to create that data pipeline and provide Data Citizens the data about air quality.

Questions this project can answer:
- What is the trend of air quality in Malaysia over the past years?
- Which is the best location to live in Malaysia in terms of air quality?
- Which months and days have poor / good air quality?

## Features

- Air Quality Data scraped from [APIMS](http://apims.doe.gov.my/api_table.html)

- Air Quality Data is updated from X/04/2022 - present. Data from 2005 - 2022 is also available*
- Query Data through [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction)

- Daily workflow that can be observed through Prefect Cloud
- Alerts whenever a workflow has failed

## Air Quality Data

![image](https://user-images.githubusercontent.com/19585239/195292149-ac7e48d1-8d98-4b85-9533-8616aca9a58d.png)
![image](https://user-images.githubusercontent.com/19585239/195292738-30a6ae22-a266-4456-9634-fc5ee7217ebc.png)

As can be seen above, the air quality data has a few attributes and it's meaning can be referenced by the government website [APIMS](http://apims.doe.gov.my/api_table.html)

**Note 1:** The data is updated every day at 12.00am GMT+8 (Malaysia Time)

**Note 2:** The data is for the past 24 hours from 12.00AM on the date. If the date is 12/10/2022, the data is from 1.00AM on the 11th of October to 12.00AM on the 12th of October. Eg: _2100 on 12/10/2022 is 9.00pm on the 11th of October while _900 on 12/10/2022 is 9.00am on the 12th of October.

## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`PROJECT_ID=quality-of-life-364309`

`DATASET=air_quality`

#### Provide your own values for the variables below

`WORKLOAD_IDENTITY_PROVIDER`

`SERVICE_ACCOUNT`

Setup reference: [Google Auth GitHub Actions](https://github.com/google-github-actions/auth#setup)

## Installation

Python 3 is required for this project.

```bash
  git clone <url>
  cd <project-name>

  python -m venv venv     # create a virtual environment
  venv\Scripts\Activate.ps1     # activate the virtual environment

  pip install -r requirements.txt   # installing dependencies

  python main.py    # runs the main code of this project
```

## Querying Data (for Data Citizens)

This project uses BigQuery as a Data Warehouse, giving you the power to use SQL to query data.

`PROJECT_ID=quality-of-life-364309`

`DATASET=air_quality`

There are 2 tables in the `air_quality` dataset, `locations_air_quality` and `state_locations`

Example SQL Statements

```bash
  SELECT * FROM `quality-of-life-364309.air_quality.locations_air_quality` AS air_q
  INNER JOIN `quality-of-life-364309.air_quality.state_locations` AS state_locs
  ON air_q.Location = state_locs.Location LIMIT 1000
```

You can play around with BigQuery SQL using Kaggle

[Kaggle](https://www.kaggle.com/code/dansbecker/getting-started-with-sql-and-bigquery)

#### Have Fun!

## Contributing

Contributions are always welcome!

#### Several things can be improved:

- Add all the historical air quality data ([Hong Lim's Kaggle Dataset](https://www.kaggle.com/datasets/honglim/malaysia-air-quality-index-2017), [YnShung's API Malaysia](https://github.com/ynshung/api-malaysia))

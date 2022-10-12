# Malaysia Air Quality & Temperature Data Pipeline

Temperature data and air quality data is provided on government & 3rd party sites in Malaysia. Unfortunately, finding the latest and constantly updating datasets on this topic is difficult. 

This project aims to create that data pipeline and provide data citizens the data they need in an accessible way.


## Features

- Air Quality Data scraped from [APIMS](http://apims.doe.gov.my/api_table.html)
- Temperature Data (Todo)
- Query Data through [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction)

## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`PROJECT_ID=quality-of-life-364309`
 
`DATASET=air_quality`

#### Provide your own values for the variables  below
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
- Create pipeline for temperature data
- Add all the historical air quality data ([Hong Lim's Kaggle Dataset](https://www.kaggle.com/datasets/honglim/malaysia-air-quality-index-2017))
- There are 3-4 stations which collect air-quality data not provided by the main URL we scrape (can refer to main.py). Need to combine the scraped data from that with the main table to complete the pipeline.


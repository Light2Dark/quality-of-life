from google.cloud import bigquery
import os

project = os.environ["PROJECT"]
dataset = os.environ["DATASET"]


def print_result(table, table_id: str):
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def save_state_locs_bq(df):
    table_name = os.environ["TABLE_STATE"]
    table_id = "{}.{}.{}".format(project, dataset, table_name)
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Waits for table load to complete.

    table = client.get_table(table_id)  # Make an API request.
    print_result(table, table_id)


def save_air_quality_bq(df):
    table_name = os.environ["TABLE_AIR_QUALITY"]
    table_id = "{}.{}.{}".format(project, dataset, table_name)
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATE),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        # write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Waits for table load to complete.

    table = client.get_table(table_id)  # Make an API request.
    print_result(table, table_id)

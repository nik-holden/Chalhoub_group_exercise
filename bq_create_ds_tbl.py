from google.cloud import bigquery
import os

# Construct a BigQuery client object
path = os.path.dirname(__file__) #path of py file
file = "constant-racer-267008-eab473065348.json"  # Service key file used for athentication
file_loc = "{path}/{file}".format(path=path, file=file)  # concatinated file location and name
client = bigquery.Client.from_service_account_json(file_loc)  # Create bigquery client object

# Global variable
project_id = client.project  # Get the project ID based on the authenticated session



def create_dataset(ds_name):
    """create_dataset function creates a dataset in Google Cloud BigQuery under a certain project instace.
    This function takes one argument: The name of the dataset. THe project instance ID is obtain from the authentication credentials"""
    
    datasets = list(client.list_datasets())  # Get datasets already part of a project
    dataset_list = []  # Create an empty list

    # Append individual data sets to the empty list
    for dataset in datasets:
        dataset_list.append(dataset.dataset_id)

    # Check if dataset to  be created already exists and create it if it doesn't
    if ds_name not in dataset_list:
        print("{} does not exist. Creating now".format(ds_name))
        ds_id = "{}.{}".format(project_id, ds_name)

        dataset = bigquery.Dataset(ds_id)

        dataset.location="US"

        dataset = client.create_dataset(dataset)
        print("Created dataset {}.{}".format(project_id, dataset.dataset_id))
    else:
        print("Dataset {} exists".format(ds_name))


def create_table(ds_name, table_name):
    """create_table function creates a table on Google Cloud BigQuery for a specified dataset under a specific project instance.
    This function takes 2 argumens: the dataset name and the table name to be created.  The project instance is obtains with the auth credentials"""
    tables = list(client.list_tables(ds_name))  # Get tables part of a dataset
    table_list = []
    table_id = "{}.{}.{}".format(project_id, ds_name, table_name)

    schema = [
        bigquery.SchemaField("record_key", "STRING", mode="REQUIRED", description="Key for record"),
        bigquery.SchemaField("submission_id", "STRING", mode="NULLABLE", description="ID of submission"),
        bigquery.SchemaField("subreddit_name", "STRING", mode="NULLABLE", description="Display name of subreddit the submission was sourced from"),
        bigquery.SchemaField("submission_created_date", "DATE", mode = "NULLABLE", description = "Date submission was created"),
        bigquery.SchemaField("submission_created_ts", "INTEGER", mode="NULLABLE", description="UTC timestamp when submission was created"),
        bigquery.SchemaField("submission_score", "INTEGER", mode="NULLABLE", description="Score of submission at the time the record data was extracted"),
        bigquery.SchemaField("submission_num_comments", "INTEGER", mode = "NULLABLE", description = "Number of comments at the time the record data was extracted"),
        bigquery.SchemaField("submission_author", "STRING", mode="NULLABLE", description="Reddit user creating the submission"),
        bigquery.SchemaField("submission_title", "STRING", mode="NULLABLE", description="Title of the submission"),
        bigquery.SchemaField("submission_url", "STRING", mode="NULLABLE", description="URL of the submission"),
        bigquery.SchemaField("extract_ts", "TIMESTAMP", mode="NULLABLE", description="Date-time the record was extracted"),
    ]

    for table in tables:
        table_list.append(table.table_id)

    if table_name not in table_list:
        print("{table_name} is not part of the {ds_name} dataset.  Creating it now".format(table_name=table_name, ds_name=ds_name))
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    else:
        print("{} exists".format(table_name))

create_table("chalhoub_exercise", "subreddit_daily_top_10")

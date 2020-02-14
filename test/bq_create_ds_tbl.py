from google.cloud import bigquery
project_id = 'chal-cbq'

client = bigquery.Client.from_service_account_json("C:/Python/chal-cbq-b350ab7026d3.json")
def create_dataset(dataset_name):
    dataset_id = "{}.{}".format(client.project, dataset_name)

    dataset = bigquery.Dataset(dataset_id)

    dataset.location="US"

    dataset = client.create_dataset(dataset)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

#create_dataset("new_dataset4")

def create_table(datasetid, table_name):
    table_id = "{}.{}.{}".format(client.project, datasetid, table_name)

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

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#create_table("new_dataset4","test_tbl_2")

table_id = "{}.{}.{}".format(client.project, "new_dataset4","test_tbl_3")

schema = [
    bigquery.SchemaField("name", "STRING")
]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)
from google.cloud import bigquery
import os

path = os.path.dirname(__file__)
file = "constant-racer-267008-eab473065348.json"

file_loc = "{path}/{file}".format(path=path, file=file)

print(file_loc)

client = bigquery.Client.from_service_account_json(file_loc)

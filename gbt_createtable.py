
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family


client = Client.from_service_account_json('C:/Python/bigtable-268017-b752c68d956f.json', admin=True)
instance = client.instance('bt-again')

table_name = instance.table('reddit-table')
table_name.create()

print('ok')



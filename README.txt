Program detail

#main.sh
Shell script used to launch main.py from a CRON job.  A shell script is used due to the presence of the non standard library PRAW in reddit_api.py

#main.py
This script accepts 2 arguments: dataset ID and target table name, to write the reddit data to the target Google Cloud BigQuery table.  This script calls reddit_api.py and passes the 2 arguments to it.

#reddit_api.py
This script retrieves the specified number of submissions from the specified sub reddit.  The submissions returned are written to a Google Cloud BigQuery table. 
The Python Reddit API Wrapper (PRAW) library is used to facilitate API calls to Reddit.  PRAW uses the praw.ini file to authenticate the connection to reddit.The path this script is launched from must match the path the file is located in.

 A validation is done to check if the target table is in the dataset provided by main.py.  If either value does not match the values provided an error is thrown.  While this has not been enabled in this script, calling the bq_create_ds_tbl.py will create them.

#bq_create_ds_tbl.py
This script contains 2 functions: create_dataset and create_table.

create_dataset takes one argument, the name of a dataset to create. The dataset is created based on the project ID obtained from authentication

Create_table takes 2 arguments: the dataset name under which the table will be created and the table name to create. The dataset is created based on the project ID obtained from authentication

#requirments.txt 
This file lists the non standard libraries that require installation to run the programs

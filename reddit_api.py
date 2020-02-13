#############################
#
#Date Created: 20200213
#
#Author: Nik Holden
#
#Date last reviewed: 20200213
#
#############################

#modules
#base modules
import logging
from datetime import datetime
from datetime import date
import time

#packages requiring an install
from google.cloud import bigquery #Used to access GC BigQuery
import praw #Python reddit API wrapper (PRAW)

#Configure logging
LOG_FORMAT = '%(levelname)s, %(asctime)s, %(message)s'
logging.basicConfig(filename = 'redditapi.log', level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger()

#General variables

extract_ts = datetime.now() #Date-time of record extraction
extract_date = date.today() #Date of record extraction
limit = 10 #Limit of records
def record_extraction():
    #Start of program
    logger.info("Program has started.")
    prog_start = datetime.now()

    #Create a PRAW instance
    reddit = praw.Reddit('chal2')
    subreddit = reddit.subreddit('dubai')
    filtered_subreddit = subreddit.top('month', limit=limit) #Apply filter for top submissions

    #Create a GC BigQuery object
    client = bigquery.Client.from_service_account_json("C:/Python/chal-cbq-b350ab7026d3.json") #this will need to be removed when py app de

    #Dataset and table within GC BQ to insert records into
    datasetid = "new_dataset4"
    table_name = "test_tbl_2"

    table_id = "{}.{}.{}".format(client.project, datasetid, table_name)

    n = 0 # count of loop instances
    w = 1 #count of successful writes
    #Create a formated series of objects to be inserted in the GC BigQuery table

    for submission in filtered_subreddit:
        n += 1 #Increment is placed at the top of the loop to provide an accurate count of records

        record = "{}#{}#{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
        subreddit.display_name.lower(), extract_date, submission.id, #Key for record
        submission.id, #ID of submission
        subreddit.name, #Name of subreddit
        date.fromtimestamp(submission.created_utc), #Date submission was created
        submission.created_utc, #UTC timestamp when submission was created
        submission.score, #Score of submission at the time the record data was extracted
        submission.num_comments, #Number of comments at the time the record data was extracted
        submission.author, #Reddit user creating the submission
        submission.title, #Title of the submission
        submission.url, #URL of the submission
        extract_ts) #Date-time the record was extracted

        try:
            record2 = tuple(record.split("|")) # Create tuple to be added to set for insert
        except:
            logger.error("Error occured when creating a tuple.")

        try:
            record_to_insert = [record2] #Create set containing tuple ready for insert
        except:
            logger.error("Error occured when creating a list.")

        table = client.get_table(table_id) #retireve table schema
        errors = client.insert_rows(table, record_to_insert) #insert records into table

        if errors == []:
            w += 1
        else:
            logger.info('An error occurred with record {}: {}'.format(n, record_to_insert))

    #end of program
    prog_end = datetime.now()
    logger.info("Program has ended. {} records out of {} were written. Run time was {}".format(w-1, limit, prog_end - prog_start))
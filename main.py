
#!/usr/bin/python

import reddit_api
import sys

def main(datasetid, table_name):
    print("main.py calling reddit")
    reddit_api.record_extraction(datasetid, table_name)

if __name__ == '__main__':
    datasetid = sys.argv[1]
    table_name = sys.argv[2]
    main(datasetid, table_name)

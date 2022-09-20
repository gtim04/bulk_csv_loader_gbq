import pandas as pd
import os
import time
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
from os import getenv

#env initialization
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

#constant vars from env
TARGET_FOLDER = getenv('TARGET_FOLDER')
PROJECT_ID = getenv('PROJECT_ID')
DATASET = getenv('DATASET')
SVC_ACNT_PATH = getenv('SVC_ACNT_PATH')

#load to GBQ
def loadToBq(dataframe,table_name,ad_trigger,fm):
    client = bigquery.Client.from_service_account_json(SVC_ACNT_PATH)

    job_config = bigquery.LoadJobConfig(
        autodetect=ad_trigger, allow_quoted_newlines=True, source_format=bigquery.SourceFormat.CSV
    )

    project_id = PROJECT_ID #order 1
    ds_name = DATASET #order 2

    table_id = "{}.{}.{}".format(project_id,ds_name,table_name)

    load_job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    message = "Created table" if fm == True else "Loaded table"
    print("{} {}. Current rows: {}".format(message,table_name,destination_table.num_rows))
#end load to GBQ

#looping all files in the directory
directory = os.fsencode(TARGET_FOLDER)

for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".csv"):
        filepath = TARGET_FOLDER + filename
        table_name = filename.split(".")[0]
        df=pd.read_csv(filepath, low_memory=False, engine='c')
        n = 100000  #chunk row size
        list_df = [df[i:i+n] for i in range(0,df.shape[0],n)]
        for index, chunks in enumerate(list_df):
            if index == 0:
                loadToBq(chunks,table_name,True,True)
            else:
                loadToBq(chunks,table_name,False,False)
            print("Sleeping... (failure prevention)")
            time.sleep(10)
        continue
    else:
        continue
#end looping files in dir
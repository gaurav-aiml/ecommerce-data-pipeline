#imports to facilitate automation according to date
from datetime import datetime, timedelta, date


import logging
#main airflow import
from airflow import models, DAG

#imports for PySpark Job and managing the DataProc cluster for the same
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator

from airflow.operators.python_operator import PythonOperator
#imports for transfering files from GCS to BIGQuery (bq load opeartions)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

#imports to run bash script
from airflow.operators.bash_operator import BashOperator

from airflow.models import *

#imports to read environment variables set in the airflow environment
from airflow.models import Variable

#imports for the Trigger Rule ()
from airflow.utils.trigger_rule import TriggerRule

import time

current_date = str(date.today())
BUCKET = "gs://gmp-etl/"
HIVE_JOB = BUCKET + "real-time-user-logs/hive-sql-scripts/hive-sql-cart-script.py"

DEFAULT_ARGS = {
    "owner":"airflow",
    "start_date":datetime(2021,3,25),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "project_id":"de-on-gcp",
    "schedule_interval":"@hourly"
}

def test():
    logging.log(level = logging.DEBUG, msg="Hellloo Worlddd!!!")

with DAG("test1", default_args=DEFAULT_ARGS, catchup=True) as dag:
    submit_pyspark = PythonOperator(
        task_id="test1",
        python_callable=test,
        # cluster_name="user-logs-hive",
        # region="us-central1"
    ) 
    submit_pyspark.dag = dag
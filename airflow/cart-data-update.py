#imports to facilitate automation according to date
from datetime import datetime, timedelta, date

#main airflow import
from airflow import models, DAG

#imports for PySpark Job and managing the DataProc cluster for the same
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator

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
dag = DAG(
    "cart-data-update",
    schedule_interval='*/15 * * * *',
    catchup=False,
    start_date=datetime(2021,3,25))
    # start_date=datetime.utcnow())

submit_pyspark = DataProcPySparkOperator(
        task_id="run-cart-hive-qry",
        main=HIVE_JOB,
        cluster_name="user-logs-hive",
        region="us-central1",
        dag=dag
    )
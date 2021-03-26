gcloud dataproc jobs submit hive \
--cluster user-logs-hive \
--region us-central1 \
--execute "CREATE EXTERNAL TABLE raw_data_logs (category STRING,date_time timestamp,type STRING,pid INTEGER,state STRING,sub_cat STRING, ip_address STRING)
STORED AS PARQUET
LOCATION 'gs://gmp-etl/real-time-user-logs/raw-data-dump';"
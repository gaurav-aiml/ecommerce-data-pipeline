gcloud dataproc jobs submit hive \
--cluster user-logs-hive \
--region us-central1 \
--execute "DROP TABLE raw_data_logs;"
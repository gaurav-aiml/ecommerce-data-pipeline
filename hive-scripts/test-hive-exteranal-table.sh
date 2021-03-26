gcloud dataproc jobs submit hive \
    --cluster user-logs-hive \
    --region us-central1 \
    --execute "SELECT date(date_time) as event_date FROM raw_data_logs WHERE pid IS NOT NULL GROUP BY date_time LIMIT 10;"
import os 
from google.cloud import bigquery

def upload_visited_gcs_to_bq(data, context):
    client = bigquery.Client()

    dataset_id = "ecom_user_data"

    dataset_ref = client.dataset(dataset_id)

    job_config = bigquery.job.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True 
    job_config.ignore_unknown_values = True
    job_config.source_format = bigquery.SourceFormat.PARQUET


    uri = 'gs://gmp-etl/real-time-user-logs/hive-processed-visits-output/*.parquet'
    
    load_job = client.load_table_from_uri(uri,dataset_ref.table('processed_user_visits'),job_config=job_config)
    
    print("Starting Job {}".format(load_job.job_id))
    load_job.result()
    print("Job Finished")
    destination_table = client.get_table(dataset_ref.table('processed_user_visits'))
    print('Loaded {} rows.' .format(destination_table.num_rows))


def upload_cart_gcs_to_bq(data, context):
    client = bigquery.Client()

    dataset_id = "ecom_user_data"

    dataset_ref = client.dataset(dataset_id)

    job_config = bigquery.job.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True 
    job_config.ignore_unknown_values = True
    job_config.source_format = bigquery.SourceFormat.PARQUET


    uri = 'gs://gmp-etl/real-time-user-logs/hive-processed-cart-output/*.parquet'
    
    load_job = client.load_table_from_uri(uri,dataset_ref.table('processed_user_cart'),job_config=job_config)
    
    print("Starting Job {}".format(load_job.job_id))
    load_job.result()
    print("Job Finished")
    destination_table = client.get_table(dataset_ref.table('processed_user_cart'))
    print('Loaded {} rows.' .format(destination_table.num_rows))

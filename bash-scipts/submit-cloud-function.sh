#!/bin/sh

FUNCTION1="upload_visited_gcs_to_bq"
FUNCTION2="upload_cart_gcs_to_bq"
BUCKET="gs://gmp-etl"

gcloud functions deploy ${FUNCTION1} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize && 

gcloud functions deploy ${FUNCTION2} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
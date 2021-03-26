gcloud dataproc clusters create user-logs-hive \
--region us-central1 \
--zone us-central1-a \
--scopes default \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 100 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 100 \
--image-version 1.4-debian9
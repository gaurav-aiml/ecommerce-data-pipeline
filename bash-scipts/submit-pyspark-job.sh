gcloud dataproc jobs submit pyspark \
--properties ^#^spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5 \
../spark-scripts/kafka-to-gcp.py \
--cluster user-logs-spark \
--region us-central1
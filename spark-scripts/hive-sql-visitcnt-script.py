from pyspark.sql import HiveContext
from pyspark import SparkContext

import time

sc = SparkContext()
hc = HiveContext(sc)

folder = str(int(round(time.time() * 1000)))

#When PID is null, that indicates that a product page was visited.
query = """ SELECT
                date(date_time) as event_date,
                type,
                state,
                category,
                COUNT(*) as visit_count
            FROM
                raw_data_logs
            WHERE
                pid IS NULL
            GROUP BY
                event_date,type,state,category
        """

#running the above HiveQL query using the Hive Context.
visit_count = hc.sql(query)

#gathering output from all worker nodes and writing the dataframe in parquet format to GCS bucket.
visit_count.coalesce(1).write.format("parquet").save("gs://gmp-etl/real-time-user-logs/hive-processed-visits-output/"+folder)
from pyspark.sql import HiveContext
from pyspark import SparkContext

import time

sc = SparkContext()
hc = HiveContext(sc)

folder = str(int(round(time.time() * 1000)))

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
visit_count = hc.sql(query)
visit_count.coalesce(1).write.format("parquet").save("gs://gmp-etl/real-time-user-logs/hive-processed-visits-output/"+folder)
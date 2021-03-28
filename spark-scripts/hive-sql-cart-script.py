from pyspark.sql import HiveContext
from pyspark import SparkContext

import time

sc = SparkContext()
hc = HiveContext(sc)

folder = str(int(round(time.time() * 1000)))

#When PID is not null, that indicates that a product has been added to the cart.
query = """ SELECT
                date(date_time) as event_date,
                type,
                state,
                category,
                COUNT(*) as num_in_cart
            FROM
                raw_data_logs
            WHERE
                pid IS NOT NULL
            GROUP BY
                event_date,type,state,category
        """

#running the above HiveQL query using the Hive Context
visit_count = hc.sql(query)

#gathering output from all worker nodes and writing the dataframe in parquet format to GCS bucket
visit_count.coalesce(1).write.format("parquet").save("gs://gmp-etl/real-time-user-logs/hive-processed-cart-output/"+folder)

try:
    import os
    import sys
    import uuid
    import hashlib
    from dateutil import parser
    from datetime import datetime
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from datetime import datetime
    from functools import reduce
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType
    from faker import Faker
except Exception as e:
    pass
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
    .getOrCreate()


class Dedup(object):
    def __init__(self, primary_key, precom_key, spark_df):
        self.primary_key = primary_key
        self.precom_key = precom_key
        self.spark_df = spark_df

    def run(self):
        """
        This will return pyspark dataframe
        :return: df
        """
        """Converting Datetime into timestamp"""
        str_date_to_unix_timestamp_fn = F.udf(Dedup.str_date_to_unix_timestamp, T.IntegerType())
        spark_df = self.spark_df.withColumn('ts', str_date_to_unix_timestamp_fn(F.col(self.precom_key)))

        spark_df_indexed = spark_df.withColumn("seq_num", row_number().over(Window.orderBy("ts")))
        print(spark_df_indexed.show())
        print("\n")

        temp_df = spark_df_indexed.groupBy(F.col(self.primary_key)).agg(
            max('ts').alias('ts'),
            max('seq_num').alias('m_seq_num')
        )
        print(temp_df.show())
        print("\n")

        temp_df_joined = spark_df_indexed.join(temp_df, spark_df_indexed.seq_num == temp_df.m_seq_num, "inner")
        print(temp_df_joined.show())
        print("\n")

        temp_df_joined = temp_df_joined.drop(*["m_seq_num", "ts", "seq_num"])
        return temp_df_joined

    @staticmethod
    def str_date_to_unix_timestamp(datetime_str):
        return int(parser.parse(datetime_str).timestamp())


spark_df = spark.createDataFrame(
    data=[
        (1, "insert 1", 111, "2020-01-06 12:12:12", "INSERT"),
        (1, "update 1", 111, "2020-01-06 12:12:15", "UPDATE"),
        (1, "update 2", 111, "2020-01-06 12:12:17", "UPDATE"),
        (2, "insert 2", 22, "2020-01-06 12:12:18", "INSERT"),
        (3, "insert 2", 22, "2020-01-06 12:12:28", "DELETE"),
    ],
    schema=["record_id", "message", "precomb", "event_timestamp", "event_name"])
"""
Docs
primary_key is comma seperated values based on this hash is calculated  
precomb key is date attributes based on which dedup will be done
"""
helper = Dedup(primary_key="record_id", precom_key="event_timestamp", spark_df=spark_df)
df = helper.run()
print(df.show())

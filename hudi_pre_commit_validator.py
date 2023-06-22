try:
    from pyspark.sql import SparkSession
    import os
    import sys
    import uuid
    from datetime import datetime
    from faker import Faker
except Exception as e:
    print("Error: ", e)

hudi_version = '0.13.1'
jar_file = 'hudi-spark3.3-bundle_2.12-0.14.0-SNAPSHOT.jar'
os.environ['PYSPARK_SUBMIT_ARGS'] = f"--jars {jar_file} pyspark-shell"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.jars', jar_file) \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

db_name = "hudidb"
table_name = "pre_commit_validator"
recordkey = 'uuid'
precombine = 'precomb'
method = 'upsert'
table_type = "COPY_ON_WRITE"
validator_query = """SELECT COUNT(*) FROM <TABLE_NAME> WHERE message IS NULL;"""
path = f"file:///C:/tmp/{db_name}/{table_name}"

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': method,
    'hoodie.datasource.write.precombine.field': precombine,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    "hoodie.precommit.validators": "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator",
    "hoodie.precommit.validators.equality.sql.queries": validator_query
}

spark_df = spark.createDataFrame(data=[
    (1, "This is APPEND 1", 111, "1"),
    (2, "This is APPEND 2", 222, "2"), ],
    schema=["uuid", "message", "precomb", "partition"])

spark_df.write.format("hudi").options(**hudi_options).mode("append").save(path)
spark.read.format("hudi").load(path).createOrReplaceTempView("hudi_snapshots")
spark.sql("select * from hudi_snapshots").show(truncate=False)


spark_df = spark.createDataFrame(
    data=[
        (4, None, 444, None),
        (5, "This is APPEND 5", 555, "5"),
    ],
    schema=["uuid", "message", "precomb", "partition"])
spark_df.show()
spark_df.write.format("hudi").options(**hudi_options).mode("append").save(path)
spark.read.format("hudi").load(path).createOrReplaceTempView("hudi_snapshots")
spark.sql("select * from hudi_snapshots").show(truncate=False)

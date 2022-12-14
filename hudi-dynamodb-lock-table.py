try:
    import os
    import sys
    import uuid

    import boto3

    import pyspark
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, asc, desc
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.context import GlueContext

    from faker import Faker

    print("All modules are loaded .....")

except Exception as e:
    print("Some modules are missing {} ".format(e))


# ----------------------------------------------------------------------------------------
#                 Settings
# -----------------------------------------------------------------------------------------

database_name1 = "hudidb"
table_name = "hudi_table"
base_s3_path = "s3a://glue-learn-begineers"
final_base_path = "{base_s3_path}/{table_name}".format(
    base_s3_path=base_s3_path, table_name=table_name
)
curr_session = boto3.session.Session()
curr_region = curr_session.region_name

# ----------------------------------------------------------------------------------------------------
global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            (
                x,
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                faker.random_int(min=10000, max=150000),
                faker.random_int(min=18, max=60),
                faker.random_int(min=0, max=100000),
                faker.unix_time()
            ) for x in range(10)
        ]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.hive.convertMetastoreParquet','false') \
        .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true') \
        .getOrCreate()
    return spark



spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)

"""
CHOOSE ONE 
"hoodie.datasource.write.storage.type": "MERGE_ON_READ",
"hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
"""


hudi_options = {
    'hoodie.table.name': table_name,
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    'hoodie.datasource.write.recordkey.field': 'emp_id',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'state',

    'hoodie.datasource.hive_sync.enable': 'true',
    "hoodie.datasource.hive_sync.mode":"hms",
    'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
    'hoodie.datasource.hive_sync.database': database_name1,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true',


    'hoodie.write.concurrency.mode' : 'optimistic_concurrency_control'
    ,'hoodie.cleaner.policy.failed.writes' : 'LAZY'
    ,'hoodie.write.lock.provider' : 'org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider'
    ,'hoodie.write.lock.dynamodb.table' : 'hudi-lock-table'
    ,'hoodie.write.lock.dynamodb.partition_key' : 'tablename'
    ,'hoodie.write.lock.dynamodb.region' : '{0}'.format(curr_region)
    ,'hoodie.write.lock.dynamodb.endpoint_url' : 'dynamodb.{0}.amazonaws.com'.format(curr_region)
    ,'hoodie.write.lock.dynamodb.billing_mode' : 'PAY_PER_REQUEST'
    ,'hoodie.bulkinsert.shuffle.parallelism': 2000


}


# ====================================================
"""Create Spark Data Frame """
# ====================================================
data = DataGenerator.get_data()

columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
df = spark.createDataFrame(data=data, schema=columns)
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(final_base_path)


# ====================================================
"""APPEND """
# ====================================================

impleDataUpd = [
    (11, "This is APPEND", "Sales", "RJ", 81000, 30, 23000, 827307999),
    (12, "This is APPEND", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]

columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
usr_up_df = spark.createDataFrame(data=impleDataUpd, schema=columns)
usr_up_df.write.format("hudi").options(**hudi_options).mode("append").save(final_base_path)


# ====================================================
"""UPDATE """
# ====================================================
impleDataUpd = [
    (3, "this is update on data lake", "Sales", "RJ", 81000, 30, 23000, 827307999),
]
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
usr_up_df = spark.createDataFrame(data=impleDataUpd, schema=columns)
usr_up_df.write.format("hudi").options(**hudi_options).mode("append").save(final_base_path)


# ====================================================
"""SOFT DELETE """
# ====================================================
# from pyspark.sql.functions import lit
# from functools import reduce
#
#
# print("\n")
# soft_delete_ds  = spark.sql("SELECT * FROM hudidb.hudi_table_rt where emp_id='4' ")
# print(soft_delete_ds.show())
# print("\n")
#
# # prepare the soft deletes by ensuring the appropriate fields are nullified
# meta_columns = ["_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key","_hoodie_partition_path", "_hoodie_file_name"]
# excluded_columns = meta_columns + ["ts", "emp_id", "partitionpath"]
# nullify_columns = list(filter(lambda field: field[0] not in excluded_columns, list(map(lambda field: (field.name, field.dataType), soft_delete_ds.schema.fields))))
#
# soft_delete_df = reduce(lambda df, col: df.withColumn(col[0], lit(None).cast(col[1])),
#                         nullify_columns, reduce(lambda df,col: df.drop(col[0]), meta_columns, soft_delete_ds))
#
#
# soft_delete_df.write.format("hudi").options(**hudi_options).mode("append").save(final_base_path)
#
#
#



# # ====================================================
# """HARD DELETE """
# # ====================================================
#
# ds = spark.sql("SELECT * FROM hudidb.hudi_table_rt where emp_id='2' ")
#
# hudi_hard_delete_options = {
#     'hoodie.table.name': table_name,
#     'hoodie.datasource.write.recordkey.field': 'emp_id',
#     'hoodie.datasource.write.table.name': table_name,
#     'hoodie.datasource.write.operation': 'delete',
#     'hoodie.datasource.write.precombine.field': 'ts',
#     'hoodie.upsert.shuffle.parallelism': 2,
#     'hoodie.insert.shuffle.parallelism': 2
# }
#
# ds.write.format("hudi").options(**hudi_hard_delete_options).mode("append").save(final_base_path)

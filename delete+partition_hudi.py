"""
Author : Soumil Nitin Shah
Email shahsoumil519@gmail.com
--additional-python-modules  | faker==11.3.0
--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

--datalake-formats | hudi
"""

try:
    import sys, os, ast, uuid, boto3, datetime, time, re, json
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    from pyspark.sql.functions import lit, udf
    from pyspark.sql.types import StringType
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

DYNAMODB_LOCK_TABLE_NAME = 'hudi-lock-table'
curr_session = boto3.session.Session()
curr_region = curr_session.region_name

# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
    ],
)

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


def upsert_hudi_table(glue_database, table_name,
                      record_id, precomb_key, table_type, spark_df,partition_feild,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_dynamodb_lock,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_feild,
        "hoodie.datasource.hive_sync.partition_fields": partition_feild,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    hudi_dynamo_db_based_lock = {
        'hoodie.write.concurrency.mode': 'optimistic_concurrency_control'
        , 'hoodie.cleaner.policy.failed.writes': 'LAZY'
        , 'hoodie.write.lock.provider': 'org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider'
        , 'hoodie.write.lock.dynamodb.table': DYNAMODB_LOCK_TABLE_NAME
        , 'hoodie.write.lock.dynamodb.partition_key': 'tablename'
        , 'hoodie.write.lock.dynamodb.region': '{0}'.format(curr_region)
        , 'hoodie.write.lock.dynamodb.endpoint_url': 'dynamodb.{0}.amazonaws.com'.format(curr_region)
        , 'hoodie.write.lock.dynamodb.billing_mode': 'PAY_PER_REQUEST'

    }

    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value

        # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # if DynamoDB based lock enabled use dynamodb as lock table
    if enable_dynamodb_lock == "True" or enable_dynamodb_lock == "true" or enable_dynamodb_lock == True:
        for key, value in hudi_dynamo_db_based_lock.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:

        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


global faker
faker = Faker()


def get_customer_data(total_customers=2):
    customers_array = []
    for i in range(0, total_customers):
        customer_data = {
            "customer_id": i,
            "name": faker.name(),
            "state": faker.state(),
            "city": faker.city(),
            "email": faker.email(),
            "ts": datetime.now().isoformat().__str__()
        }
        customers_array.append(customer_data)
    return customers_array


# total_customers = 100
#
# customer_data = get_customer_data(total_customers=total_customers)
# spark_df_customers = spark.createDataFrame(data=[tuple(i.values()) for i in customer_data],
#                                            schema=list(customer_data[0].keys()))
# print(spark_df_customers.show(truncate=False))

path = "s3://soumilshah-hudi-demos/silver/table_name=customers"


# upsert_hudi_table(
#     glue_database="hudidb",
#     table_name="customers",
#     record_id="customer_id",
#     precomb_key="ts",
#     partition_feild='state',
#     table_type="COPY_ON_WRITE",
#     method='upsert',
#     index_type="BLOOM",
#     enable_partition="True",
#     enable_cleaner="True",
#     enable_hive_sync="True",
#     enable_dynamodb_lock="False",
#     use_sql_transformer="False",
#     sql_transformer_query="default",
#     target_path=path,
#     spark_df=spark_df_customers,
# )


# ============DELETE PARTITION ================
# ============DELETE PARTITION ================

print(
    "Connecticut", spark.read.format("hudi").load(path).where("state='Connecticut'").count()
)

hudi_options = {}

try:
    spark.createDataFrame([], StructType([])) \
        .write \
        .format("org.apache.hudi") \
        .options(**hudi_options) \
        .option("hoodie.datasource.hive_sync.enable", False) \
        .option("hoodie.datasource.write.operation", "delete_partition") \
        .option("hoodie.datasource.write.partitions.to.delete", "state=Connecticut") \
        .mode("append") \
        .save(path)
    print("2.. ")
except Exception as e:
    print("Error 2", e)


print(
    "Connecticut ** ", spark.read.format("hudi").load(path).where("state='Connecticut'").count()
)

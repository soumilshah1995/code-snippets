try:
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3
    from functools import reduce
    from pyspark.sql import Row

    import uuid
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()

global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            (
                uuid.uuid4().__str__(),
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                str(faker.random_int(min=10000, max=150000)),
                str(faker.random_int(min=18, max=60)),
                str(faker.random_int(min=0, max=100000)),
                str(faker.unix_time()),
                faker.email(),
                faker.credit_card_number(card_type='amex'),
                faker.date()
            ) for x in range(100)
        ]


# ============================== Settings =======================================
db_name = "hudidb"
table_name = "employees"
recordkey = 'emp_id'
precombine = "ts"
PARTITION_FIELD = 'state'
path = "s3://XXXl/test1/"
method = 'bulk_insert'
table_type = "COPY_ON_WRITE"

hudi_part_write_config = {
    'className': 'org.apache.hudi',

    'hoodie.table.name': table_name,
    'hoodie.datasource.write.table.type': table_type,
    'hoodie.datasource.write.operation': method,
    'hoodie.bulkinsert.sort.mode': "NONE",
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.precombine.field': precombine,

    # --------------Hive Sync --------------
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.support_timestamp': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,
    # -----------------------------------------------------
    # -----------------------------------------------------
    "hoodie.clean.automatic": "true"
    , "hoodie.clean.async": "true"
    , "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS'
    , "hoodie.cleaner.fileversions.retained": "3"
    , "hoodie-conf hoodie.cleaner.parallelism": '200'
    , 'hoodie.cleaner.commits.retained': 5
    # -----------------------------------------------------

}

# ====================================================================================


data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card",
           "date"]
spark_df = spark.createDataFrame(data=data, schema=columns)
spark_df.write.format("hudi").options(**hudi_part_write_config).mode("append").save(path)

commits = list(map(lambda row: row[0], spark.sql(f"call show_commits('{db_name}.{table_name}', 5)").collect()))
print(" #1# commits", commits)
query_save_point = f"call create_savepoint('{db_name}.{table_name}', '{commits[0]}')"
execute_save_point = spark.sql(query_save_point)
print(execute_save_point.show())
print(spark.sql(f"""SELECT COUNT(*) FROM {db_name}.{table_name}""").show())
print("-------------1 complete -----------------")

# =================================================================

# INSERT 2 BATCH AND TAKE SAVE POINTS

# ====================================================================================


data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card",
           "date"]
spark_df = spark.createDataFrame(data=data, schema=columns)
spark_df.write.format("hudi").options(**hudi_part_write_config).mode("append").save(path)

commits = list(map(lambda row: row[0], spark.sql(f"call show_commits('{db_name}.{table_name}', 5)").collect()))
print(" #2# commits", commits)
query_save_point = f"call create_savepoint('{db_name}.{table_name}', '{commits[0]}')"
execute_save_point = spark.sql(query_save_point)
print(execute_save_point.show())
print(spark.sql(f"""SELECT COUNT(*) FROM {db_name}.{table_name}""").show())
print("-------------2 complete -----------------")

# =================================================================

# INSERT 3 BATCH AND TAKE SAVE POINTS

# ====================================================================================

data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card",
           "date"]
spark_df = spark.createDataFrame(data=data, schema=columns)
spark_df.write.format("hudi").options(**hudi_part_write_config).mode("append").save(path)

commits = list(map(lambda row: row[0], spark.sql(f"call show_commits('{db_name}.{table_name}', 5)").collect()))
print(" #3# commits", commits)
query_save_point = f"call create_savepoint('{db_name}.{table_name}', '{commits[0]}')"
execute_save_point = spark.sql(query_save_point)
print(execute_save_point.show())
print(spark.sql(f"""SELECT COUNT(*) FROM {db_name}.{table_name}""").show())
print("-------------2 complete -----------------")

# =================================================================


# =================================================================

# SHOW ALL SAVE POINTS

# ======================================================================

varSP2 = spark.sql(f"call show_savepoints('{db_name}.{table_name}')")
print(varSP2.show())
print("-------------show_savepoints complete -----------------")

# =================================================================

# DELETING NEWER CHECK POINTS TO ROLL TO OLD VERSION

# ======================================================================
print("delete_savepoint")
commits = list(map(lambda row: row[0], spark.sql(f"call show_commits('{db_name}.{table_name}', 5)").collect()))
print("delete commits ", commits)
varSP0 = spark.sql(f"call delete_savepoint('{db_name}.{table_name}', '{commits[0]}')")
print(varSP0.show())
print("-------------delete_savepoint complete -----------------")


# =================================================================

# SHOW ME ALL SAVE POINTS

# ======================================================================

varSP2 = spark.sql(f"call show_savepoints('{db_name}.{table_name}')")
print(varSP2.show())
print("-------------show_savepoints complete -----------------")

# =================================================================

# ROLLBACK

# ======================================================================

print("RollBack *********")
commits = list(map(lambda row: row[0], spark.sql(f"call show_savepoints('{db_name}.{table_name}')").collect()))
print("roll back commits", commits)

try:
    spark_df = spark.sql(f"call rollback_to_savepoint('{db_name}.{table_name}', '{commits[0]}')")
    print(spark_df.show())
except Exception as e:
    print("**error",e)


# ====================================================================================

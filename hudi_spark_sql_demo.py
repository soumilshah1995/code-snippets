"""
--conf | spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats | hudi

https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-table-formats-apache-hudi.html

"""
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

# Get command-line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create Spark session and Glue context
spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Database and table information
DB_NAME = "hudidb"
TABLE_NAME = "customer"
TABLE_S3_LOCATION = "s3://soumilshah-hudi-demos/silver/"


# Check if the database already exists
existing_databases = [db.name for db in spark.catalog.listDatabases()]

if DB_NAME not in existing_databases:
    # Create the database
    query = f"CREATE DATABASE IF NOT EXISTS {DB_NAME} LOCATION '{TABLE_S3_LOCATION}'"
    spark.sql(query)
    print("Database '{}' created.".format(DB_NAME))

# Check if the table already exists
existing_tables = [tbl.name for tbl in spark.catalog.listTables(DB_NAME)]
if TABLE_NAME not in existing_tables:
    # Create the table using Hudi
    query = f"""
     CREATE TABLE {DB_NAME}.{TABLE_NAME} (
            customer_id string,
            name string,
            age int,
            email string
        )
        USING HUDI
        TBLPROPERTIES (
            primaryKey = 'customer_id',
            type = 'mor'
        )
    """
    spark.sql(query)
    print("Table '{}' created.".format(TABLE_NAME))

# Insert data into the customer table
data = [
    ("C001", "Soumil Shah", 29, "dummy.soumil@example.com"),
    ("C002", "Nitin", 65, "dummy.nitin@example.com")
]

spark.createDataFrame(data, ["customer_id", "name", "age", "email"]).createOrReplaceTempView("temp_table")

# Insert data into the table using Spark SQL
query = f"""
    INSERT INTO TABLE {DB_NAME}.{TABLE_NAME}
    SELECT * FROM temp_table
    """
result = spark.sql(query)
result.show()



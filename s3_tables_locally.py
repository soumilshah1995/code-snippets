from pyspark.sql import SparkSession
import os

# Set JAVA_HOME if needed
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

# Define variables
WAREHOUSE_PATH = "XXXX"

# Define packages
packages = [
    "com.amazonaws:aws-java-sdk-bundle:1.12.661",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.29.38",
    "com.github.ben-manes.caffeine:caffeine:3.1.8",
    "org.apache.commons:commons-configuration2:2.11.0",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
]

# Initialize SparkSession with required configurations
spark = SparkSession.builder \
    .appName("iceberg_lab") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", WAREHOUSE_PATH) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.catalog.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket.client.region", "us-east-1") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.example_namespace")
spark.sql("SHOW NAMESPACES IN s3tablesbucket").show()
spark.sql("SHOW TABLES in s3tablesbucket.example_namespace").show()

spark.sql("""CREATE TABLE IF NOT EXISTS s3tablesbucket.example_namespace.table1
 (id INT,
  name STRING,
  value INT)
  USING iceberg
  """)

spark.sql("""
    INSERT INTO  s3tablesbucket.example_namespace.table1
    VALUES
        (1, 'ABC', 100),
        (2, 'XYZ', 200)
""")
spark.sql("SELECT * FROM  s3tablesbucket.example_namespace.table1 ").show()

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import input_file_name
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext  # Correct import for GlueContext
from awsglue.job import Job

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()

path = "s3://XX/zone=raw/"

# Step 1: Read the data into a DynamicFrame
glue_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [path],
        "recurse": True,
    },
    format="json",
)

spark_df = glue_df.toDF()

spark_df.createOrReplaceTempView("temp")

result_df = spark.sql("""
    SELECT *, 
        input_file_name() as file_path,
        regexp_extract(input_file_name(), 'year=(\\\\d+)', 1) AS year,
        regexp_extract(input_file_name(), 'month=(\\\\d+)', 1) AS month,
        regexp_extract(input_file_name(), 'day=(\\\\d+)', 1) AS day
    FROM temp
""")

result_df.select(["file_path", "year", "month", "day"]).show(truncate=False)


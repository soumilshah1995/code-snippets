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

path = "s3://XXX/sample_raw/"

# Step 1: Read the data into a DynamicFrame
glue_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [path],
        "recurse": True,
        "exclusions": ["sample_raw/order_*"]
    },
    format="json",
)

spark_df = glue_df.toDF()
spark_df.show()

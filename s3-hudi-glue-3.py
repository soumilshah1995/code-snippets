import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.session import SparkSession


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args['JOB_NAME'], args)


table_name="hudi_table"
db_name = "hudidb"

# Script generated for node Amazon S3
AmazonS3_node1671052685135 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://glue-learn-begineers/data/"], "recurse": True},
    transformation_ctx="AmazonS3_node1671052685135",
)

# Script generated for node Rename Field
RenameField_node1671052718523 = RenameField.apply(
    frame=AmazonS3_node1671052685135,
    old_name="id",
    new_name="pk",
    transformation_ctx="RenameField_node1671052718523",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1671052735917 = ApplyMapping.apply(
    frame=RenameField_node1671052718523,
    mappings=[
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("address", "string", "address", "string"),
        ("text", "string", "text", "string"),
        ("pk", "string", "pk", "string"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1671052735917",
)

# Script generated for node Apache Hudi Connector 0.10.1 for AWS Glue 3.0
ApacheHudiConnector0101forAWSGlue30_node1671052751031 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchemaApplyMapping_node1671052735917,
        connection_type="marketplace.spark",
        connection_options={
            "path": "s3://glue-learn-begineers/hudi/",
            "connectionName": "hudi-connection",

            "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
            'className': 'org.apache.hudi',
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': 'pk',
            'hoodie.datasource.write.table.name': table_name,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.precombine.field': 'first_name',


            'hoodie.datasource.hive_sync.enable': 'true',
            "hoodie.datasource.hive_sync.mode":"hms",
            'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
            'hoodie.datasource.hive_sync.database': db_name,
            'hoodie.datasource.hive_sync.table': table_name,
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
        },
        transformation_ctx="ApacheHudiConnector0101forAWSGlue30_node1671052751031",
    )
)

job.commit()

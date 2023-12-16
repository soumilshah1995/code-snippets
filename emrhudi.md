# Hudi Integration with Apache Spark on Amazon EMR

This guide provides step-by-step instructions to set up and use Apache Hudi with Apache Spark on Amazon EMR. Apache Hudi is a data storage system that provides efficient incremental data processing using Apache Spark.

## Step 1: Copy Hudi JAR to HDFS

```bash
# SSH into EMR
hdfs dfs -mkdir -p /apps/hudi/lib
hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
```


This step involves creating a directory on HDFS and copying the Hudi Spark bundle JAR to that directory.


Step 2: Start Jupyter Notebook on EMR

Open JupyterHub in your browser.
Enter the following credentials:
Username: jovyan
Password: jupyter
Start a new Jupyter PySpark notebook.
Configure Spark with Hudi:


```

%%configure
{
"conf": {
"spark.jars": "hdfs:///apps/hudi/lib/hudi-spark-bundle.jar",
"spark.serializer": "org.apache.spark.serializer.KryoSerializer",
"spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
"spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
}}

```

Run the provided PySpark code to create a DataFrame and write it to Hudi:
```

impleDataUpd = [
    (3, "This is APPEND", "Sales", "RJ", 81000, 30, 23000, 827307999),
    (4, "This is APPEND", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]

columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]

spark_df = spark.createDataFrame(data=impleDataUpd, schema=columns)

db_name = "hudidb"
table_name = "customers"
recordkey = 'emp_id'
precombine = 'ts'
path = "s3://soumil-dev-bucket-1995/hudi/"
method = 'upsert'
table_type = "COPY_ON_WRITE"

hudi_options = {
    "hoodie.datasource.write.storage.type": table_type,
    'className': 'org.apache.hudi',
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': method,
    'hoodie.datasource.write.precombine.field': precombine,
    'hoodie.datasource.hive_sync.enable': 'true',
    "hoodie.datasource.hive_sync.mode": "hms",
    'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true',
}

spark_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(path)
```


Step 3: Query Data in Hive

Go to HUE and select Hive.
Run the following query to view the data in the Hudi table:

```
ELECT * FROM hudidb.customers;
```

Step 4: Query Data in Presto

SSH into EMR:

```

# SSH into EMR
cd /usr/lib/presto/bin
presto-cli

CONNECT localhost:8080;
SHOW SCHEMAS FROM hive;
SELECT * FROM hudidb.customers;


```

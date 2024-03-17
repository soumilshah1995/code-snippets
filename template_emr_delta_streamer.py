import os, time, uuid, json
import boto3
from dotenv import load_dotenv

load_dotenv(".env")


def check_job_status(client, run_id, applicationId):
    response = client.get_job_run(applicationId=applicationId, jobRunId=run_id)
    return response['jobRun']['state']


def lambda_handler(event, context):
    # Create EMR serverless client object
    client = boto3.client("emr-serverless",
                          aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
                          aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
                          region_name=os.getenv("DEV_REGION"))

    # Extracting parameters from the event
    jar = event.get("jar", [])
    # Add --conf spark.jars with comma-separated values from the jar object
    spark_submit_parameters = ' '.join(event.get("spark_submit_parameters", []))  # Convert list to string
    spark_submit_parameters = f'--conf spark.jars={",".join(jar)} {spark_submit_parameters}'  # Join with existing parameters

    arguments = event.get("arguments", {})
    job = event.get("job", {})

    # Extracting job details
    JobName = job.get("job_name")
    ApplicationId = job.get("ApplicationId")
    ExecutionTime = job.get("ExecutionTime")
    ExecutionArn = job.get("ExecutionArn")

    # Processing arguments
    entryPointArguments = []
    for key, value in arguments.items():
        if key == "hoodie-conf":
            # Extract hoodie-conf key-value pairs and add to entryPointArguments
            for hoodie_key, hoodie_value in value.items():
                entryPointArguments.extend(["--hoodie-conf", f"{hoodie_key}={hoodie_value}"])
        elif isinstance(value, bool):
            # Add boolean parameters without values if True
            if value:
                entryPointArguments.append(f"--{key}")
        else:
            entryPointArguments.extend([f"--{key}", f"{value}"])

    # Starting the EMR job run
    response = client.start_job_run(
        applicationId=ApplicationId,
        clientToken=str(uuid.uuid4()),
        executionRoleArn=ExecutionArn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': "command-runner.jar",
                'entryPointArguments': entryPointArguments,
                'sparkSubmitParameters': spark_submit_parameters
            },
        },
        executionTimeoutMinutes=ExecutionTime,
        name=JobName
    )

    if job.get("JobStatusPolling") == True:
        # Polling for job status
        run_id = response['jobRunId']
        print("Job run ID:", run_id)

        polling_interval = 3
        while True:
            status = check_job_status(client=client, run_id=run_id, applicationId=ApplicationId)
            print("Job status:", status)
            if status in ["CANCELLED", "FAILED", "SUCCESS"]:
                break
            time.sleep(polling_interval)  # Poll every 3 seconds

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


event = {
    "jar": [
        "/usr/lib/hudi/hudi-utilities-bundle.jar"
    ],
    "spark_submit_parameters": [
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "--conf spark.sql.hive.convertMetastoreParquet=false",
        "--conf mapreduce.fileoutputcommitter.marksuccessfuljobs=false",
        "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer"
    ],
    "arguments": {
        "table-type": "COPY_ON_WRITE",
        "op": "UPSERT",
        "enable-sync": True,
        "sync-tool-classes": "io.onetable.hudi.sync.OneTableSyncTool",
        "source-ordering-field": "replicadmstimestamp",
        "source-class": "org.apache.hudi.utilities.sources.ParquetDFSSource",
        "target-table": "invoice",
        "target-base-path": "s3://<BUCKET>/testcases/",
        "payload-class": "org.apache.hudi.common.model.AWSDmsAvroPayload",
        "hoodie-conf": {
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
            "hoodie.datasource.write.recordkey.field": "invoiceid",
            "hoodie.datasource.write.partitionpath.field": "destinationstate",
            "hoodie.deltastreamer.source.dfs.root": "s3://<BUCKET>/test/",
            "hoodie.datasource.write.precombine.field": "replicadmstimestamp",
            "hoodie.database.name": "hudidb",
            "hoodie.datasource.hive_sync.enable": True,
            "hoodie.datasource.hive_sync.table": "invoice",
            "hoodie.datasource.hive_sync.partition_fields": "destinationstate",

        }
    },
    "job": {
        "job_name": "delta_streamer_invoice",
        "created_by": "Soumil Shah",
        "created_at": "2024-03-20",
        "ApplicationId": "<ID>",
        "ExecutionTime": 600,
        "JobActive": True,
        "schedule": "0 8 * * *",
        "JobStatusPolling": True,
        "JobDescription": "Ingest data from parquet source",
        "ExecutionArn": "<ARN>",
    }
}

lambda_handler(event=event, context=None)

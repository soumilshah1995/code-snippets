import boto3
import json
import duckdb


def get_table_metadata(table_bucket_arn, namespace, table_name):
    try:
        response = boto3.client('s3tables').get_table(
            tableBucketARN=table_bucket_arn, namespace=namespace, name=table_name
        )
        return response['metadataLocation']
    except Exception as e:
        raise RuntimeError(f"Error retrieving table metadata: {e}")


def query_iceberg_table_to_df(metadata_location, query):
    try:
        conn = duckdb.connect(':memory:')

        # Execute setup steps separately to avoid syntax issues
        conn.execute("SET home_directory='/tmp';")
        conn.execute("INSTALL aws;")
        conn.execute("LOAD aws;")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL iceberg;")
        conn.execute("LOAD iceberg;")
        conn.execute("CALL load_aws_credentials();")

        # Replace placeholder with the Iceberg table scan
        iceberg_table = f"iceberg_scan('{metadata_location}')"
        query = query.replace('<src>', iceberg_table)

        # Execute the query
        return conn.execute(query).fetchdf()

    except Exception as e:
        raise RuntimeError(f"Error querying Iceberg table: {e}")
    finally:
        conn.close()


def lambda_handler(event, context=None):
    try:
        metadata_location = get_table_metadata(event['table_bucket_arn'], event['namespace'], event['table'])
        df = query_iceberg_table_to_df(metadata_location, event['query'])
        return {"statusCode": 200, "body": df.to_json(orient='records', date_format='iso')}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# Test
print(lambda_handler({
    "table_bucket_arn": "XXXX",
    "namespace": "example_namespace",
    "table": "orders",
    "query": "SELECT * FROM <src> LIMIT 10"
}))

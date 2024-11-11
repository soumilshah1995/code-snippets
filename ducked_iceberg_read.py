import boto3
import duckdb
from typing import List

class DuckDBConnector:
    def __init__(self):
        self.conn = None

    def connect_and_setup(self) -> duckdb.DuckDBPyConnection:
        self.conn = duckdb.connect()
        extensions = ['iceberg', 'aws', 'httpfs']
        for ext in extensions:
            self.conn.execute(f"INSTALL {ext};")
            self.conn.execute(f"LOAD {ext};")
        self.conn.execute("CALL load_aws_credentials();")
        return self.conn

def get_latest_metadata_file(bucket_name: str, iceberg_path: str) -> str:
    s3 = boto3.client('s3')
    metadata_prefix = f"{iceberg_path.strip('/')}/metadata/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
    metadata_files = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.metadata.json')
    ]
    return f"s3://{bucket_name}/{sorted(metadata_files)[-1]}" if metadata_files else None

def main(iceberg_path: str):
    bucket_name = 'XX'
    latest_metadata_path = get_latest_metadata_file(bucket_name, iceberg_path)

    if not latest_metadata_path:
        print("No metadata files found.")
        return

    print("Latest Metadata File:", latest_metadata_path)

    connector = DuckDBConnector()
    conn = connector.connect_and_setup()

    query = f"SELECT * FROM iceberg_scan('{latest_metadata_path}')"
    result = conn.execute(query).fetchall()

    for row in result:
        print(row)

if __name__ == "__main__":
    iceberg_path = "s3://<BUCKET>snowflake/customers/"
    main(iceberg_path)

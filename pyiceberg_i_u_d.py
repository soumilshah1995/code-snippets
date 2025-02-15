import os
import pyarrow as pa
import pandas as pd
import warnings
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.expressions import EqualTo, In

# Define S3 path for the Iceberg warehouse
s3_warehouse_path = 's3://soumil-dev-bucket-1995/pyiceberg/'


def configure_iceberg(database_name: str, table_name: str, schema: Schema):
    """Configures the Iceberg catalog and creates a table if it doesn't exist."""
    print(f"Creating Iceberg Glue Catalog for database '{database_name}'...")
    catalog = GlueCatalog(
        database_name,
        **{
            "warehouse": s3_warehouse_path,
        }
    )

    # Create namespace if it doesn't exist
    print(f"Creating namespace '{database_name}' if it doesn't exist...")
    catalog.create_namespace_if_not_exists(database_name)

    # Create the table if it doesn't exist
    print(f"Creating table '{database_name}.{table_name}' if it doesn't exist...")
    iceberg_table = catalog.create_table_if_not_exists(
        identifier=f'{database_name}.{table_name}',
        schema=schema
    )

    return iceberg_table


def overwrite_data(iceberg_table, df):
    """Overwrites data in the Iceberg table."""
    print("Overwriting data in the Iceberg table...")
    iceberg_table.overwrite(df=df)


def delete_data(iceberg_table, delete_filter):
    """Deletes data from the Iceberg table based on the provided filter."""
    print(f"Deleting data where {delete_filter}...")
    iceberg_table.delete(delete_filter=delete_filter)


def append_data(iceberg_table, df):
    """Appends data to the Iceberg table."""
    print("Appending data to the Iceberg table...")
    iceberg_table.append(df=df)


def update_or_append_data(iceberg_table, pa_table, schema, pk='event_id'):
    """Efficiently updates existing records or appends new records to the Iceberg table."""
    print("Updating or appending data to the Iceberg table...")

    # Convert PyArrow Table to Pandas DataFrame
    new_df = pa_table.to_pandas()

    # Extract unique IDs from the new data
    new_ids = new_df[pk].unique().tolist()
    print(f"Unique IDs to process: {new_ids}")

    try:
        # Query existing data from the Iceberg table for these IDs
        existing_records = iceberg_table.scan(row_filter=In(pk, new_ids)).to_arrow()
        print("Successfully queried existing records.")
    except Exception as e:
        print(f"Error querying existing records: {e}")
        return

    # Convert existing records to a Pandas DataFrame
    existing_df = existing_records.to_pandas()
    print("Converted existing records to Pandas DataFrame.")

    # Merge new data with existing data to identify updates and new records
    merged_df = pd.merge(new_df, existing_df, on=pk, how='left', indicator=True)
    print("Merged new data with existing data.")

    # Identify updates and new records
    updates_df = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    new_records_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    print(f"Identified {len(updates_df)} records to update.")
    print(f"Identified {len(new_records_df)} new records to append.")

    # Batch update existing records
    if not updates_df.empty:
        try:
            print(f"Updating {len(updates_df)} records...")
            update_table = pa.Table.from_pandas(updates_df, schema=schema.as_arrow())
            iceberg_table.overwrite(df=update_table, overwrite_filter=In(pk, updates_df[pk].tolist()))
            print("Successfully updated records.")
        except Exception as e:
            print(f"Error updating records: {e}")

    # Batch append new records
    if not new_records_df.empty:
        try:
            append_table = pa.Table.from_pandas(new_records_df, schema=schema.as_arrow())
            append_data(iceberg_table, append_table)
            print("Successfully appended new records.")
        except Exception as e:
            print(f"Error appending new records: {e}")

def read_data(iceberg_table):
    """Reads data from the Iceberg table and returns it as a Pandas DataFrame."""
    print("Reading data from the Iceberg table...")
    return iceberg_table.scan().to_arrow().to_pandas()


def main():
    schema = Schema(
        NestedField(field_id=1, name='event_id', field_type=IntegerType(), required=True),
        NestedField(field_id=2, name='event_name', field_type=StringType(), required=True),
        NestedField(field_id=3, name='user_count', field_type=IntegerType(), required=True),
        NestedField(field_id=4, name='timestamp', field_type=StringType(), required=True),
    )

    # Configure Iceberg
    iceberg_table = configure_iceberg('default', 'user_events', schema)

    print(F"""
    ------------- ----------- --------- OVERWRITE ----------- ----------- -----------
    """)
    # 1. Overwrite existing records with initial event metrics
    pa_overwrite_data = pa.Table.from_pylist([
        {'event_id': 1, 'event_name': 'User Signup', 'user_count': 1500, 'timestamp': '2024-01-01T12:00:00Z'},
        {'event_id': 2, 'event_name': 'Page View', 'user_count': 3000, 'timestamp': '2024-01-01T12:05:00Z'}
    ], schema=schema.as_arrow())

    overwrite_data(iceberg_table, pa_overwrite_data)

    # Read and display data after overwriting
    df_after_overwrite = read_data(iceberg_table)
    print("Data from the table after overwriting:\n", df_after_overwrite.to_string(index=False))

    print(F"""
    ------------- ----------- --------- APPEND ----------- ----------- -----------
    """)
    pa_append_data = pa.Table.from_pylist([
        {'event_id': 3, 'event_name': 'Purchase', 'user_count': 500, 'timestamp': '2024-01-01T12:10:00Z'},
        {'event_id': 4, 'event_name': 'Login', 'user_count': 1200, 'timestamp': '2024-01-01T12:15:00Z'}
    ], schema=schema.as_arrow())

    append_data(iceberg_table, pa_append_data)

    # Read and display data after appending
    df_after_append = read_data(iceberg_table)
    print("Data from the table after appending:\n", df_after_append.to_string(index=False))

    print(F"""
    ------------- ----------- --------- DELETES ----------- ----------- -----------
    """)

    delete_data(iceberg_table, EqualTo('event_id', 2))

    df_after_deletion = read_data(iceberg_table)
    print("Data from the table after deletion:\n", df_after_deletion.to_string(index=False))

    print(F"""
    ------------- ----------- --------- UPSERTS ----------- ----------- ----------- 
    """)
    # Create a sample DataFrame with data to update or append
    input_data = pd.DataFrame([
        {'event_id': 1, 'event_name': 'User Signup Updated', 'user_count': 2500000,
         'timestamp': '2024-01-01T12:00:00Z'},
        {'event_id': 5, 'event_name': 'Logout', 'user_count': 800, 'timestamp': '2024-01-01T12:20:00Z'},
    ])

    # Read and display data before processing
    df_before_update = read_data(iceberg_table)
    print("Data from the table before processing:\n", df_before_update.to_string(index=False))

    # Convert the input DataFrame to a PyArrow Table
    pa_table = pa.Table.from_pandas(input_data)

    # Call the update or append function with PyArrow Table
    update_or_append_data(iceberg_table, pa_table, schema, pk='event_id')

    # Read and display data after processing
    df_after_update = read_data(iceberg_table)
    print("Data from the table after processing:\n", df_after_update.to_string(index=False))


if __name__ == "__main__":
    main()

%idle_timeout 2880
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

%%configure
{
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.job_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.job_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.job_catalog.warehouse=s3://<your-bucket-name>/tmp/ --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
    "--enable-glue-datacatalog": "true",
    "--iceberg_job_catalog_warehouse": "s3://<your-bucket-name>/tmp/",
    "--datalake-formats": "iceberg"
}

import time

ut = time.time()

df_products = spark.createDataFrame(
    [
        ("00001", "Heater", 250, "Electronics", ut),
        ("00002", "Thermostat", 400, "Electronics", ut),
        ("00003", "Television", 600, "Electronics", ut),
        ("00004", "Blender", 100, "Electronics", ut),
        ("00005", "Table", 150, "Furniture", ut)
    ],
    ["product_id", "product_name", "price", "category", "updated_at"],
)

path = "s3://<your-bucket-name>/tmp/products"

(
    df_products.write
    .format("iceberg")
    .mode("overwrite")
    .option("path", path)
    .saveAsTable("job_catalog.default.products")
)

# Read and show initial data
df_read = (
    spark.read
    .format("iceberg")
    .table("job_catalog.default.products")
).show()

# ======= UPDATES =======
# Define and create the updates DataFrame
df_updates = spark.createDataFrame(
    [
        ("00001", "Heater**", 250, "Electronics", ut),
    ],
    ["product_id", "product_name", "price", "category", "updated_at"]
)

# Register updates DataFrame as a temporary view
df_updates.createOrReplaceTempView("products_updates")
df_updates.show()

# Perform the merge operation
spark.sql("""
    MERGE INTO job_catalog.default.products AS target
    USING products_updates AS source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN UPDATE SET
        target.product_name = source.product_name,
        target.price = source.price,
        target.category = source.category,
        target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT (
        product_id,
        product_name,
        price,
        category,
        updated_at
    ) VALUES (
        source.product_id,
        source.product_name,
        source.price,
        source.category,
        source.updated_at
    )
""")

# Read and show updated data
df_read = (
    spark.read
    .format("iceberg")
    .table("job_catalog.default.products")
).show()

# ------- DELETES -------
spark.sql("""
DELETE FROM job_catalog.default.products
WHERE product_id = '00005'
""")

# ====== APPEND ======
ut = time.time()

df_updates = spark.createDataFrame(
    [
        ("00006", "Heater ", 250, "Electronics", ut),
    ],
    ["product_id", "product_name", "price", "category", "updated_at"]
)

df_updates.show()

df_updates.write \
    .format("iceberg") \
    .mode("append") \
    .option("path", "s3://<your-bucket-name>/tmp/products") \
    .saveAsTable("job_catalog.default.products")

# Final read to show all changes
df_read = (
    spark.read
    .format("iceberg")
    .table("job_catalog.default.products")
).show()

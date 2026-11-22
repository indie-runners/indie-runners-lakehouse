
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()

table_name = "indie_runners.bronze.core_events"
landing_path = "/Volumes/indie_runners/landing/core/events"
checkpoint_path = "/Volumes/indie_runners/bronze/checkpoints/core_events"
timeout_seconds = 600

if spark.catalog.tableExists(table_name):
    table_before = spark.read.table(table_name)
    rows_before = table_before.count()
    columns_before = table_before.columns
else:
    rows_before = 0
    columns_before = []

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(landing_path)
    .withColumn("source_file", F.col("_metadata.file_name"))
    .withColumn("ingested_at", F.current_timestamp())
)

query = (
    df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

finished = query.awaitTermination(timeout_seconds)
if not finished:
    query.stop()
    raise TimeoutError(f"Stream did not finish within {timeout_seconds} seconds")

table_after = spark.read.table(table_name)

rows_read = sum((p["numInputRows"] or 0) for p in query.recentProgress)
rows_after = table_after.count()
columns_after = table_after.columns

new_columns = [x for x in columns_after if x not in columns_before]

if new_columns:
    print(f"Columns before: {len(columns_before)}")
    print(f"Columns after: {len(columns_after)}")
    print(f"New columns: {new_columns}")
    table_after.printSchema()


print(f"Rows before: {rows_before}")
print(f"Rows after: {rows_after}")
print(f"Rows read in this run: {rows_read}")

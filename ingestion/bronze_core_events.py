
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()

table_name = "indie_runners.bronze.core_events"
landing_path = "/Volumes/indie_runners/landing/core/events"
checkpoint_path = "/Volumes/indie_runners/bronze/checkpoints/core_events"

rows_before = spark.read.table(table_name).count()

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
    .trigger(availableNow=True)
    .toTable(table_name)
)

finished = query.awaitTermination(120)
if not finished:
    query.stop()
    raise TimeoutError("Stream did not finish within 120 seconds")

rows_read = sum((p["numInputRows"] or 0) for p in query.recentProgress)
rows_after = spark.read.table(table_name).count()

print(f"Rows before: {rows_before}")
print(f"Rows after: {rows_after}")
print(f"Rows read in this run: {rows_read}")
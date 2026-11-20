from pyspark.sql import functions as F

landing_path = "/Volumes/indie_runners/landing/core/events"
checkpoint_path = "/Volumes/indie_runners/bronze/checkpoints/core_events"

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
    .toTable("indie_runners.bronze.core_events")
)

query.awaitTermination(120)
print(query.isActive)
print(query.status)
print(query.lastProgress)
print(spark.table("indie_runners.bronze.core_events").count())

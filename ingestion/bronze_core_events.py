from pyspark.sql import functions as F

landing_path = "/Volumes/indie_runners/landing/core/events"
checkpoint_path = "/Volumes/indie_runners/bronze/checkpoints/core_events"

# on readStream:   .option("cloudFiles.schemaLocation", checkpoint_path)
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(landing_path)
    .withColumn("source_file", F.col("_metadata.file_name"))
    .withColumn("ingested_at", F.current_timestamp())
)

df.printSchema()

# on writeStream:  .option("checkpointLocation", checkpoint_path)

df.writeStream.toTable("indie_runners.bronze.core_events")
    .option("checkpointLocation", checkpoint_path)

landing_path = "/Volumes/indie_runners/landing/core/events"
checkpoint_path = "/Volumes/indie_runners/bronze/checkpoints/core_events"

# on readStream:   .option("cloudFiles.schemaLocation", checkpoint_path)
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(landing_path)
)

df.printSchema()

# on writeStream:  .option("checkpointLocation", checkpoint_path)
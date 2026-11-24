"""Silver table for events: one row per id.

Write:    python silver/core_events.py
Preview:  python silver/core_events.py --preview    (writes nothing)
"""

import sys

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = DatabricksSession.builder.getOrCreate()

source_table = "indie_runners.bronze.core_events"
target_table = "indie_runners.silver.core_events"
preview_only = "--preview" in sys.argv

# Only these columns reach silver. TODO: complete from bronze.printSchema().
# Left out on purpose: current_timestamp (test column), _rescued_data.
columns = [
    "id", 
    "event_name",
    "event_display_name",
    "event_slug", "status", "country_code", "location", "notes",
    "confidence_score", "source_target",
    "created_at", "updated_at", "last_scraped_at",
    "source_file", "ingested_at",
]

bronze = spark.read.table(source_table)

# Survivor rule: newest version of the event, then the last load, then the file name.
survivor_order = Window.partitionBy("id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("ingested_at").desc(),
    F.col("source_file").desc(),
)

silver = (
    bronze
    .withColumn("rn", F.row_number().over(survivor_order))
    .filter(F.col("rn") == 1)
    .select(*columns)
    .withColumn("id", F.col("id").cast("long"))  # BigQuery INT64 is a long
)

# Checks: stop the run instead of writing a wrong table.
rows_before = bronze.count()
null_ids = bronze.filter(F.col("id").isNull()).count()
distinct_ids = bronze.select("id").distinct().count()
rows_after = silver.count()

print(f"Rows in bronze: {rows_before}")
print(f"Distinct ids in bronze: {distinct_ids}")
print(f"Rows for silver: {rows_after}")

assert null_ids == 0, f"{null_ids} bronze rows have no id"
assert rows_after == distinct_ids, f"Expected {distinct_ids} rows, got {rows_after}"

if preview_only:
    silver.printSchema()
    silver.show(5, truncate=False, vertical=True)
    print("Preview only, nothing was written")
else:
    # Full rebuild on every run. Fine at this size; a large table would use MERGE.
    (silver.write.mode("overwrite")
     .option("overwriteSchema", "True")
     .saveAsTable(target_table))
    print(f"Wrote {spark.read.table(target_table).count()} rows to {target_table}")
-- bronze load for race scraper website targets
-- incremental: only files not loaded before are picked up
-- timestampNtz is enabled because the BigQuery export contains DATETIME columns

create or refresh streaming table indie_runners.bronze.race_scraper_website_targets
tblproperties ('delta.feature.timestampNtz' = 'supported')
as select
  *,
  _metadata.file_path as source_file,
  current_timestamp() as ingested_at
from stream read_files(
  '/Volumes/indie_runners/landing/race_scraper/website_targets/',
  format => 'parquet'
);

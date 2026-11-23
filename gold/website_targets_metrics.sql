create or replace view indie_runners.gold.website_targets_metrics
with metrics
language yaml
as $$
version: 1.1
source: >
  select *
  from indie_runners.bronze.race_scraper_website_targets
  where source_file = (
    select max(source_file)
    from indie_runners.bronze.race_scraper_website_targets
  )
dimensions:
  - name: country
    expr: country
  - name: created_date
    expr: cast(created_at as date)
  - name: created_month
    expr: date_trunc('month', created_at)
measures:
  - name: total_targets
    expr: count(distinct id)
  - name: active_targets
    expr: count(distinct id) filter (where is_active)
$$;
# indie-runners-lakehouse
 
Lakehouse for European running events, built on Databricks with Delta Lake and Unity Catalog.
 
This repo holds the data platform behind the analytics of [Indie Runners](https://indierunners.co), a calendar and search for running events in Europe.

## status

Early stage. One bronze table is loaded incrementally from Parquet exports
of the race scraper. Silver and gold are empty.

Built on Databricks Free Edition. The repo holds code only, no data.

Bronze shows two ingestion patterns: a SQL streaming table where managed state is enough, and Auto Loader in PySpark where the pipeline controls checkpoint and schema itself.

## layout

Catalog: `indie_runners`

| Schema    | Holds                                                        |
|-----------|--------------------------------------------------------------|
| `landing` | Files as delivered. One volume per source system, one directory per dataset |
| `bronze`  | Raw tables, one per dataset, named source plus dataset       |
| `silver`  | Cleaned and typed tables                                     |
| `gold`    | Tables and views for analytics                               |

## setup

Needs a Databricks workspace with Unity Catalog and a SQL warehouse.

1. Run `setup/catalog.sql`. It is safe to run again.
2. Upload the export files to
   - `/Volumes/indie_runners/landing/*`
3. Run `ingestion/*`.

## planned

- More bronze tables with Auto Loader
- Silver in PySpark with data quality checks
- Gold tables and views
- Orchestration with Lakeflow Jobs
- Deployment with Declarative Automation Bundles and GitHub Actions
- Metric views as the semantic layer
- dbt
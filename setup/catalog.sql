--initial setup of medallion architecture

--catalog
create catalog if not exists indie_runners;

--schemas
create schema if not exists indie_runners.landing;
create schema if not exists indie_runners.bronze;
create schema if not exists indie_runners.silver;
create schema if not exists indie_runners.gold;

--volumes
create volume if not exists indie_runners.landing.race_scraper;
create volume if not exists indie_runners.landing.core;
--NOTE: one volume per source system, one directory per dataset, created on upload
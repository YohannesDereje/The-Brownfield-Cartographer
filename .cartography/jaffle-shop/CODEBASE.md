# Codebase Context

## Architecture Overview

The architecture could not be synthesized from available evidence.

## Critical Path

- .github/workflows/cd_prod.yml (0.023256)
- .github/workflows/cd_staging.yml (0.023256)
- .github/workflows/ci.yml (0.023256)
- .github/workflows/scripts/dbt_cloud_run_job.py (0.023256)
- .pre-commit-config.yaml (0.023256)

## Data Sources & Sinks

**Sources**
- .github/workflows/cd_prod.yml
- .github/workflows/cd_staging.yml
- .github/workflows/ci.yml
- .github/workflows/scripts/dbt_cloud_run_job.py
- .pre-commit-config.yaml
- Taskfile.yml
- cast_to_date
- compute_booleans
- customer_order_count
- customer_orders_summary
- days
- dbt_project.yml
- ecom.raw_customers
- ecom.raw_items
- ecom.raw_orders
- ecom.raw_products
- ecom.raw_stores
- ecom.raw_supplies
- joined
- macros/cents_to_dollars.sql
- macros/generate_schema_name.sql
- models/marts/customers.yml
- models/marts/locations.yml
- models/marts/order_items.yml
- models/marts/orders.yml
- models/marts/products.yml
- models/marts/supplies.yml
- models/staging/__sources.yml
- models/staging/stg_customers.yml
- models/staging/stg_locations.yml
- models/staging/stg_order_items.yml
- models/staging/stg_orders.yml
- models/staging/stg_products.yml
- models/staging/stg_supplies.yml
- order_items_summary
- order_supplies_summary
- package-lock.yml
- packages.yml
- renamed
- seeds/jaffle-data/raw_customers.csv
- seeds/jaffle-data/raw_items.csv
- seeds/jaffle-data/raw_orders.csv
- seeds/jaffle-data/raw_products.csv
- seeds/jaffle-data/raw_stores.csv
- seeds/jaffle-data/raw_supplies.csv
- source

**Sinks**
- .github/workflows/cd_prod.yml
- .github/workflows/cd_staging.yml
- .github/workflows/ci.yml
- .github/workflows/scripts/dbt_cloud_run_job.py
- .pre-commit-config.yaml
- Taskfile.yml
- dbt_project.yml
- macros/cents_to_dollars.sql
- macros/generate_schema_name.sql
- models/marts/customers.yml
- models/marts/locations.yml
- models/marts/order_items.yml
- models/marts/orders.yml
- models/marts/products.yml
- models/marts/supplies.yml
- models/staging/__sources.yml
- models/staging/stg_customers.yml
- models/staging/stg_locations.yml
- models/staging/stg_order_items.yml
- models/staging/stg_orders.yml
- models/staging/stg_products.yml
- models/staging/stg_supplies.yml
- package-lock.yml
- packages.yml
- seeds/jaffle-data/raw_customers.csv
- seeds/jaffle-data/raw_items.csv
- seeds/jaffle-data/raw_orders.csv
- seeds/jaffle-data/raw_products.csv
- seeds/jaffle-data/raw_stores.csv
- seeds/jaffle-data/raw_supplies.csv

## Known Debt

No data available.

## High-Velocity Files

- .github/workflows/cd_prod.yml (1 changes)
- .github/workflows/cd_staging.yml (1 changes)
- .github/workflows/ci.yml (1 changes)
- .github/workflows/scripts/dbt_cloud_run_job.py (1 changes)
- .pre-commit-config.yaml (1 changes)

## Module Purpose Index

| Module | Purpose |
| --- | --- |
| .github/workflows/cd_prod.yml | No implementation evidence found |
| .github/workflows/cd_staging.yml | No implementation evidence found |
| .github/workflows/ci.yml | No implementation evidence found |
| .github/workflows/scripts/dbt_cloud_run_job.py | No implementation evidence found |
| .pre-commit-config.yaml | No implementation evidence found |
| Taskfile.yml | No implementation evidence found |
| dbt_project.yml | No implementation evidence found |
| macros/cents_to_dollars.sql | No implementation evidence found |
| macros/generate_schema_name.sql | No implementation evidence found |
| models/marts/customers.sql | No implementation evidence found |
| models/marts/customers.yml | No implementation evidence found |
| models/marts/locations.sql | No implementation evidence found |
| models/marts/locations.yml | No implementation evidence found |
| models/marts/metricflow_time_spine.sql | No implementation evidence found |
| models/marts/order_items.sql | No implementation evidence found |
| models/marts/order_items.yml | No implementation evidence found |
| models/marts/orders.sql | No implementation evidence found |
| models/marts/orders.yml | No implementation evidence found |
| models/marts/products.sql | No implementation evidence found |
| models/marts/products.yml | No implementation evidence found |
| models/marts/supplies.sql | No implementation evidence found |
| models/marts/supplies.yml | No implementation evidence found |
| models/staging/__sources.yml | No implementation evidence found |
| models/staging/stg_customers.sql | No implementation evidence found |
| models/staging/stg_customers.yml | No implementation evidence found |
| models/staging/stg_locations.sql | No implementation evidence found |
| models/staging/stg_locations.yml | No implementation evidence found |
| models/staging/stg_order_items.sql | No implementation evidence found |
| models/staging/stg_order_items.yml | No implementation evidence found |
| models/staging/stg_orders.sql | No implementation evidence found |
| models/staging/stg_orders.yml | No implementation evidence found |
| models/staging/stg_products.sql | No implementation evidence found |
| models/staging/stg_products.yml | No implementation evidence found |
| models/staging/stg_supplies.sql | No implementation evidence found |
| models/staging/stg_supplies.yml | No implementation evidence found |
| package-lock.yml | No implementation evidence found |
| packages.yml | No implementation evidence found |
| seeds/jaffle-data/raw_customers.csv | No implementation evidence found |
| seeds/jaffle-data/raw_items.csv | No implementation evidence found |
| seeds/jaffle-data/raw_orders.csv | No implementation evidence found |
| seeds/jaffle-data/raw_products.csv | No implementation evidence found |
| seeds/jaffle-data/raw_stores.csv | No implementation evidence found |
| seeds/jaffle-data/raw_supplies.csv | No implementation evidence found |

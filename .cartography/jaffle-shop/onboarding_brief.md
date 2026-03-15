# Onboarding Brief

Generated for: jaffle-shop_1773527781

## What is the primary data ingestion path?

The likely ingestion path begins from .github/workflows/cd_prod.yml and flows into downstream transformations and sinks.

Evidence:
- .github/workflows/cd_prod.yml
- .github/workflows/cd_staging.yml
- .github/workflows/ci.yml

## What are the 3-5 most critical output datasets/endpoints?

.github/workflows/cd_prod.yml, .github/workflows/cd_staging.yml, .github/workflows/ci.yml, .github/workflows/scripts/dbt_cloud_run_job.py, .pre-commit-config.yaml

Evidence:
- .github/workflows/cd_prod.yml
- .github/workflows/cd_staging.yml
- .github/workflows/ci.yml
- .github/workflows/scripts/dbt_cloud_run_job.py
- .pre-commit-config.yaml

## What is the blast radius if the most critical module fails?

Failure of .github/workflows/cd_prod.yml would likely impact connected modules on the structural critical path and downstream lineage sinks.

Evidence:
- pagerank[.github/workflows/cd_prod.yml]=0.023255813953488382
- pagerank[.github/workflows/cd_staging.yml]=0.023255813953488382
- pagerank[.github/workflows/ci.yml]=0.023255813953488382

## Where is the business logic concentrated vs. distributed?

Business logic appears most concentrated in cluster 'unassigned' (43 modules, share=1.0).

Evidence:
- cluster[unassigned]=43 modules (share=1.0)

## What has changed most frequently in the last 90 days (git velocity map)?

.github/workflows/cd_prod.yml (1 changes); .github/workflows/cd_staging.yml (1 changes); .github/workflows/ci.yml (1 changes); .github/workflows/scripts/dbt_cloud_run_job.py (1 changes); .pre-commit-config.yaml (1 changes)

Evidence:
- git_velocity[.github/workflows/cd_prod.yml]=1
- git_velocity[.github/workflows/cd_staging.yml]=1
- git_velocity[.github/workflows/ci.yml]=1
- git_velocity[.github/workflows/scripts/dbt_cloud_run_job.py]=1
- git_velocity[.pre-commit-config.yaml]=1
- git_velocity[dbt_project.yml]=1
- git_velocity[macros/cents_to_dollars.sql]=1
- git_velocity[macros/generate_schema_name.sql]=1

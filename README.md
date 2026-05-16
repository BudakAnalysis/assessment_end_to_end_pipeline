# TVMaze Data Engineering Assessment

**Author:** Samed Budak  
**Date:** May 2026

---

## What this is

An end-to-end data pipeline that pulls TV show data from the [TVMaze API](https://api.tvmaze.com),
lands it in a Bronze layer, cleans and joins it in Silver, and builds analytics-ready Gold tables.
Everything runs on Databricks with Unity Catalog, and the repo includes the CI/CD and deployment configs
you'd need to take it to production.

## Project Structure

```
├── notebooks/
│   ├── 01_bronze_ingestion.py          # API -> ADLS -> Bronze Delta tables
│   ├── 02_silver_transformations.py    # Flatten, clean, join -> fact_show_data
│   ├── 03_gold_aggregations.py         # Gold KPI tables + window functions
│   ├── 04_adf_pipeline.json           # ADF pipeline definition
│   ├── 04_adf_linked_service.json     # Databricks linked service (Key Vault-backed)
│   └── 04_adf_keyvault_ls.json        # Key Vault linked service
├── sql/
│   └── 05_sql_challenge.sql            # Window functions, joins, performance tuning
├── tests/
│   ├── conftest.py
│   └── test_data_quality.py            # DQ checks: nulls, refs, schemas, failure sim
├── cicd/
│   └── azure-pipelines.yml            # Build + Deploy (DEV live, ACC/PROD outlined)
├── bundle/
│   ├── bundle.yaml                     # Databricks Asset Bundles config
│   └── configs/
│       ├── dev.yaml
│       ├── acc.yaml
│       └── prod.yaml
├── screenshots/                        # Evidence: notebook outputs, Unity Catalog, etc.
└── README.md
```

## Assessment Coverage

| Part | What it covers | Key tech |
|------|---------------|----------|
| 1 | Bronze ingestion from REST API | requests, Delta Lake, Unity Catalog |
| 2 | Silver transformations + fact table | PySpark, broadcast joins, salting, ZORDER |
| 3 | Gold analytics layer | Window functions (dense_rank, percent_rank, cumulative sum) |
| 4 | ADF orchestration | Azure Data Factory, Key Vault integration |
| 5 | SQL challenge | Window funcs, CTEs, before/after performance tuning |
| 6 | Data quality | pytest, referential integrity, schema validation |
| 7 | CI/CD pipeline | Azure DevOps YAML, multi-stage deployment |
| 8 | Databricks Asset Bundles | DABs, multi-environment configs (dev/acc/prod) |

## How to run

**Databricks notebooks** - import `notebooks/` into your workspace and run 01 -> 02 -> 03
in sequence. Each notebook reads from the previous layer's tables.

**Tests** (requires the Unity Catalog tables to exist first):
```bash
pip install pyspark delta-spark pytest
pytest tests/ -v --tb=short
```

**DABs deployment:**
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run tvmaze-pipeline-job -t dev
```

## Design notes

- **Medallion architecture** with Bronze/Silver/Gold for clear data lineage and replayability.
- **Schema evolution** via `mergeSchema=true` - the TVMaze API adds fields occasionally,
  and this way the pipeline absorbs them without manual intervention.
- **Performance**: broadcast joins on small dimension tables, salting demo for skewed keys,
  ZORDER on join columns for Delta data skipping.
- **Governance**: Unity Catalog for access control, Key Vault for secrets management.
  No tokens or connection strings in code.
- **Testing**: pytest-based DQ checks covering nulls, uniqueness, referential integrity,
  schema structure, and an intentional failure simulation to prove the checks work.

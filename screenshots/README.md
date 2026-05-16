# Screenshots

Evidence of pipeline execution on Databricks.

## Bronze Layer
- `01_bronze_shows_output.png` - Shows table record count + schema
- `01_bronze_episodes_output.png` - Episodes table record count
- `01_bronze_cast_output.png` - Cast table record count
- `01_sample_data.png` - Sample data from all three Bronze tables
- `01_schema_evolution.png` - Schema evolution demo (mergeSchema)
- `01_describe_history.png` - Delta DESCRIBE HISTORY output
- `01_unity_catalog_tables.png` - Unity Catalog table listing

## Silver Layer
- `02_silver_shows.png` - Flattened silver_shows output
- `02_genre_normalization.png` - Genre normalization (explode_outer)
- `02_fact_show_data.png` - Denormalized fact table
- `02_salting_demo.png` - Salted join output
- `02_optimize_zorder.png` - OPTIMIZE + ZORDER results
- `02_validation_counts.png` - Record counts for all Silver tables

## Gold Layer
- `03_episodes_per_season.png` - Episodes per season with rankings
- `03_top_cast.png` - Top cast members ranked
- `03_genre_statistics.png` - Genre distribution with share percentages
- `03_show_dashboard.png` - All-in-one KPI dashboard table
- `03_show_dashboard_kpis.png` - Additional dashboard KPIs
- `03_describe_history.png` - Gold table Delta history
- `03_describe_extended.png` - Gold table extended metadata
- `03_all_tables.png` - All tables across Bronze/Silver/Gold

## SQL Challenge
- `05_window_functions.png` - RANK, DENSE_RANK, LAG/LEAD results
- `05_season_trajectory.png` - Season premiere vs finale comparison
- `05_show_report.png` - Comprehensive show report (4-way join)
- `05_network_comparison.png` - Network-level aggregations
- `05_cte_optimized.png` - CTE-based query (after optimization)
- `05_execution_plan.png` - EXPLAIN EXTENDED output

## Data Quality
- `06_dq_real_data 1.png` - DQ checks on real data (part 1)
- `06_dq_real_data 2.png` - DQ checks on real data (part 2)
- `06_dq_failure_simulation.png` - Failure simulation with injected bad data
- `06_dq_summary.png` - DQ summary (pass/fail counts)

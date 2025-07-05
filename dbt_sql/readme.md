# SOFA dbt Project

This project calculates the Sequential Organ Failure Assessment (SOFA) score for ICU patients using modular, testable dbt models.  
It uses MIMIC-III or similar EHR data, and is organized for maintainability, clinical transparency, and data quality.

## Project Structure

- `models/staging/`: All modular transformations from source/raw tables.
- `models/marts/`: Final analytic outputs and clinical scores (e.g., `sofa.sql`).
- `models/schema.yml`: Centralized documentation and data tests.
- `macros/`: Custom dbt macros for advanced SQL logic.
- `seeds/`: CSV files loaded directly as tables for reference or lookups.
- `snapshots/`: Slowly changing dimension (SCD) tracking logic.
- `analyses/`: One-off SQL for data exploration.
- `tests/`: Custom data quality assertions.
- `target/`: Compiled SQL and artifacts (auto-generated).

## Getting Started

1. Install dbt:  
   `pip install dbt-bigquery` (or `dbt-postgres`, `dbt-snowflake`, etc.)
2. Clone this repo and install dependencies:  
   `dbt deps`
3. Configure your database profile in `~/.dbt/profiles.yml`.
4. Run models:  
   `dbt run`
5. Test data:  
   `dbt test`
6. Generate docs:  
   `dbt docs generate` then `dbt docs serve`

## Data Sources

See `staging/` models for all data extracted from EHR source tables, with detailed lineage and documentation in `schema.yml`.

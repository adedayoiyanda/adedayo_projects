# State Revenue ETL Pipeline

An end-to-end data engineering pipeline that automates the extraction, transformation, and delivery of government revenue collection data.

## Architecture

```
MariaDB (source)
      │
      ▼
Python ETL (incremental loads)
      │
      ▼
PostgreSQL (data warehouse)
      │
      ▼
dbt (staging views + mart tables)
      │
      ├──▶ Metabase (dashboards)
      ├──▶ Daily email alerts (pipeline status)
      └──▶ Weekly CSV reports (email delivery)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Source database | MariaDB |
| Warehouse | PostgreSQL |
| Transformation | dbt (dbt-postgres) |
| ETL language | Python (stdlib only, no pandas) |
| Email delivery | Brevo API |
| Dashboards | Metabase |
| Scheduler | Windows Task Scheduler |

## Features

- **Incremental ETL** with watermark tracking — only new/changed rows are loaded on each run
- **Full-refresh** for small reference tables (truncate and reload)
- **Slowly changing dimension (SCD)** handling for agent records using `ON CONFLICT DO UPDATE`
- **Automated daily pipeline** — ETL → dbt → email alert with run summary
- **Weekly CSV reports** delivered by email with multiple attachments
- **6 source tables** loaded from MariaDB into PostgreSQL
- **10+ dbt models** across staging (views) and marts (materialized tables)

## Project Structure

```
etl/
├── incremental_load.py   # ETL: MariaDB → PostgreSQL (all tables)
├── pipeline.py           # Orchestrator: ETL → dbt → email alert
├── csv_report.py         # Weekly CSV report generator and emailer
├── email_alert.py        # Brevo email helper
├── schedule_task.ps1     # Windows Task Scheduler setup
└── .env.example          # Environment variable template

revenue_pipeline/         # dbt project
├── models/
│   ├── staging/          # Thin views over raw tables (column renames, casts)
│   └── marts/            # Business-ready materialized tables
└── dbt_project.yml
```

## dbt Models

**Staging**
- `stg_agent_transactions` — revenue transactions from field agents
- `stg_rev_sub_newcode` — revenue code reference table
- `stg_bus_ticketing_transaction` — bus fare transactions
- `stg_concession_permit` — permit rates for vehicle and market traders
- `stg_enumeration` — taxpayer enumeration records
- `stg_merchant_user` — agent/merchant user accounts

**Marts**
- `fct_transactions` — enriched transaction fact table
- `agg_revenue_daily` — daily revenue aggregated by MDA, LGA, channel
- `fct_concession_permits` — permit payments with variance analysis
- `agg_concession_by_plancode` — permit aggregates by plan and category
- `agg_combined_revenue_daily` — unified daily revenue across all sources
- `fct_agent_enumeration` — agent performance by enumeration count and revenue type

## Getting Started

1. Copy `.env.example` to `.env` and fill in your credentials
2. Install dependencies:
   ```
   pip install psycopg2-binary mysql-connector-python python-dotenv dbt-postgres
   ```
3. Run the ETL:
   ```
   python etl/incremental_load.py
   ```
4. Run dbt:
   ```
   cd revenue_pipeline && dbt run
   ```
5. Run the full pipeline (ETL + dbt + email alert):
   ```
   python etl/pipeline.py
   ```

\# Consumer Finance Health Monitor (Fantastic Guacamole)



End-to-end analytics engineering portfolio project: ingest public economic indicators, model them in a medallion warehouse (`bronze` → `silver` → `gold`), and publish dashboard-ready monthly KPIs.



\## What this project demonstrates



\- \*\*Data ingestion\*\*: Python pulls FRED API payloads into SQL Server (`bronze`).

\- \*\*Medallion modeling\*\*: SQL transforms raw JSON into typed time series (`silver`) and monthly KPI facts (`gold`).

\- \*\*Operational rigor\*\*: run logging + validation snapshots documented over time.

\- \*\*Business relevance\*\*: consumer finance stress monitoring using public macro/credit signals.



\## Problem statement



Organizations need a repeatable way to monitor \*\*consumer financial stress signals\*\* (rates, labor, credit growth, delinquency) without ad hoc spreadsheets.



This repo implements a small warehouse + pipeline that standardizes those signals into a single monthly fact table suitable for BI dashboards.



\## Data sources



\- \*\*FRED (Federal Reserve Economic Data)\*\*: time series observations via API.



> Note: this project uses public aggregate time series (not individual-level data).



\## Architecture



\### Medallion layers



\- \*\*`bronze`\*\*: raw API JSON payloads landed for replay/audit.

\- \*\*`silver`\*\*: typed, conformed observations (`series\_id + observation\_date` grain).

\- \*\*`gold`\*\*: monthly KPI fact table aligned to `gold.dim\_date`.



\### Key database objects



\- `fantastic\_guacamole.bronze.fred\_observation\_raw`

\- `fantastic\_guacamole.silver.fred\_series`

\- `fantastic\_guacamole.silver.fred\_observation`

\- `fantastic\_guacamole.gold.dim\_date`

\- `fantastic\_guacamole.gold.fact\_consumer\_finance\_monthly`

\- `fantastic\_guacamole.ops.pipeline\_run\_log`

\- `fantastic\_guacamole.ops.silver\_load\_tracker`



\## Repo map



\- `src/`: Python ingestion + utilities

\- `SQL Server Warehouse/`: SQL DDL + stored procedures

\- `docs/`: narrative build log + validation evidence (`run\_history.md`)

\- `data/`: small exported profiling outputs (optional)



\## Quickstart (local)



\### Prerequisites



\- SQL Server (local) + database `fantastic\_guacamole`

\- Python 3.12+

\- ODBC Driver for SQL Server (`ODBC Driver 17` or `18`)



\### Configure secrets



1\. Copy `.env.example` → `.env`

2\. Set `FRED\_API\_KEY=...`



\### Install Python dependencies



```bash

python -m pip install python-dotenv requests pandas sqlalchemy pyodbc


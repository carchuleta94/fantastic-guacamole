# Run History

This log is intentionally simple and readable.
Each entry is a quick project snapshot: **what we did, what happened, and what comes next**.

---

## Snapshot Template

### [Date] [Step Name]
- **Step:** What was executed.
- **Outcome:** What happened (success/failure + short summary).
- **Evidence:** Tables/files/queries that prove the outcome.
- **Notes:** Important context, tradeoffs, or issues.
- **Next:** Immediate next action.

---

## 2026-05-04 Bronze Raw Load (Initial)
- **Step:** Ran `src/load_bronze_fred_raw.py` to ingest raw FRED payloads for selected series.
- **Outcome:** Bronze ingestion completed and run logging captured in DB.
- **Evidence:** New records in `bronze.fred_observation_raw` and `ops.pipeline_run_log`.
- **Notes:** Bronze intentionally stores full JSON payloads; flattening occurs in silver.
- **Next:** Parse and upsert into `silver.fred_series` and `silver.fred_observation`.

---

## 2026-05-04 Silver Load from Bronze (Stored Proc)
- **Step:** Executed `ops.usp_load_silver_from_bronze` to parse `bronze.fred_observation_raw.response_json` into typed rows and upsert into `silver.fred_series` + `silver.fred_observation` (batched at 1000 observation rows per batch).
- **Outcome:** Silver load completed successfully for run `288B3439-85C8-4454-BD46-F4F711975457`.
- **Evidence:**
  - `ops.silver_load_tracker`: final status `SUCCESS` for pipeline `load_silver_from_bronze` (`silver_load_tracker_id = 1`, `total_rows_staged = 61577`, `total_rows_inserted = 61577`, `total_rows_updated = 0`).
  - `ops.pipeline_run_log`: success summary row present for same run with staged/insert totals.
  - `silver.fred_observation` by-series counts reconcile to `61577` total rows.

![Proc Validation](proc_validation.png)

```sql
SELECT TOP 50 *
FROM ops.silver_load_tracker
ORDER BY silver_load_tracker_id DESC;

SELECT TOP 50 *
FROM ops.pipeline_run_log
ORDER BY run_log_id DESC;

SELECT series_id, COUNT(*) AS row_count
FROM silver.fred_observation
GROUP BY series_id
ORDER BY series_id;
```

- **Notes:** Bronze remains raw JSON payloads; silver stores flattened, typed observations with `source_raw_id` lineage back to bronze `raw_id`.
- **Next:** Run silver validation spot checks and lock monthly aggregation rules for gold.

---

## 2026-05-04 Silver Validation and Aggregation Decisions
- **Step:** Ran validation checks from `SQL Server Warehouse/silver_validation_queries.sql` after executing `ops.usp_load_silver_from_bronze`, including missingness checks, duplicate/date integrity checks, and April 2026 spot checks.
- **Outcome:** Silver quality checks passed and monthly aggregation logic for gold was finalized.
- **Evidence:**
  - `suspicious_missing_value_mismatch = 0`
  - `null_dates = 0`
  - Missingness rates were plausible for source frequency (daily market series ~4.2-4.3%; monthly/quarterly series near 0).

```sql
-- Spot Check 1: daily rates (Apr 2026) month_avg vs last value in month
DECLARE @y INT = 2026;
DECLARE @m INT = 4;
DECLARE @month_start date = DATEFROMPARTS(@y, @m, 1);
DECLARE @month_end   date = EOMONTH(@month_start);

WITH d AS (
    SELECT *
    FROM fantastic_guacamole.silver.fred_observation
    WHERE series_id IN ('DGS10','DGS2','DFF','SOFR')
      AND observation_date >= @month_start
      AND observation_date <= @month_end
      AND is_missing = 0
      AND observation_value IS NOT NULL
)
SELECT
    series_id,
    AVG(CAST(observation_value AS FLOAT)) AS month_avg,
    MAX(CASE WHEN observation_date = last_day.obs_date
             THEN CAST(observation_value AS FLOAT) END) AS last_value_in_month,
    MIN(observation_date) AS first_obs_date_in_month,
    MAX(observation_date) AS last_obs_date_in_month,
    COUNT(*) AS obs_days_in_month
FROM d
CROSS APPLY (
    SELECT MAX(observation_date) AS obs_date FROM d x WHERE x.series_id = d.series_id
) last_day
GROUP BY series_id
ORDER BY series_id;
```

![Spot Check 1](spot_check_1.png)

```sql
-- Spot Check 2: weekly credit-card balances (Apr 2026)
DECLARE @y2 INT = 2026;
DECLARE @m2 INT = 4;
DECLARE @month_start2 date = DATEFROMPARTS(@y2, @m2, 1);
DECLARE @month_end2   date = EOMONTH(@month_start2);

SELECT
    observation_date,
    observation_value
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id = 'CCLACBW027SBOG'
  AND observation_date BETWEEN @month_start2 AND @month_end2
  AND is_missing = 0
  AND observation_value IS NOT NULL
ORDER BY observation_date;
```

![Spot Check 2](spot_check_2.png)

```sql
-- Spot Check 3: monthly series availability and recent values
SELECT
    series_id,
    MAX(observation_date) AS latest_observation_date
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id IN ('UNRATE','TOTALSL')
  AND is_missing = 0
  AND observation_value IS NOT NULL
GROUP BY series_id
ORDER BY series_id;

SELECT
    series_id,
    observation_date,
    observation_value
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id IN ('UNRATE','TOTALSL')
  AND is_missing = 0
  AND observation_value IS NOT NULL
  AND observation_date >= '2025-01-01'
ORDER BY series_id, observation_date DESC;
```

![Spot Check 3](spot_check_3.png)

- **Notes:** Spot check 3 confirmed the April monthly query returning 0 rows was a release-timing issue (not a transform defect). Gold should preserve nulls for months without published values.
- **Next:** Build gold monthly procedure using locked rules:
  - Daily/weekly series -> monthly `AVG`
  - Monthly series (`TOTALSL`, `UNRATE`) -> native monthly value
  - Quarterly delinquency (`DRCCLACBS`) -> quarter-end month only (no forward fill in V1)
  - Derived spread -> `t10y2y_avg = dgs10_avg - dgs2_avg`

---

## SQL Snippet: Latest Run Summary

Run this in SQL Server to quickly summarize recent pipeline activity.

```sql
SELECT TOP 50
    run_log_id,
    pipeline_name,
    run_id,
    series_id,
    status,
    row_count,
    http_status_code,
    started_utc_dt,
    finished_utc_dt,
    message
FROM ops.pipeline_run_log
ORDER BY run_log_id DESC;
```

## SQL Snippet: Final Status By Run

```sql
WITH ranked AS (
    SELECT
        pipeline_name,
        run_id,
        status,
        message,
        started_utc_dt,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name, run_id
            ORDER BY run_log_id DESC
        ) AS rn
    FROM ops.pipeline_run_log
)
SELECT
    pipeline_name,
    run_id,
    status AS final_status,
    started_utc_dt AS last_log_ts_utc,
    message AS final_message
FROM ranked
WHERE rn = 1
ORDER BY last_log_ts_utc DESC;
```


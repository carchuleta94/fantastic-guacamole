# Run History

## 2026-05-04 Bronze Raw Load (Initial)
- **Step:** Ran `src/load_bronze_fred_raw.py` to ingest raw FRED payloads for selected series.
- **Outcome:** Bronze ingestion completed and run logging captured in DB.
- **Evidence:** New records in `bronze.fred_observation_raw` and `ops.pipeline_run_log`.
- **Notes:** Bronze intentionally stores full JSON payloads; flattening occurs in silver.
- **Next:** Parse and upsert into `silver.fred_series` and `silver.fred_observation`.

![Bronze run log](docs/images/bronze_run_log.png)

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


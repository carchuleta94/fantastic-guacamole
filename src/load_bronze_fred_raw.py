import json
import os
import uuid

import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    raise ValueError("FRED_API_KEY not found in .env")

SERIES_IDS = [
    "DGS10",
    "DGS2",
    "DFF",
    "SOFR",
    "CCLACBW027SBOG",
    "TOTALSL",
    "UNRATE",
    "DRCCLACBS",
]

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# If needed, switch to ODBC Driver 18 for SQL Server.
SQL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=fantastic_guacamole;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


def log_run(
    cursor,
    pipeline_name: str,
    run_id: str,
    status: str,
    series_id: str = None,
    message: str = None,
    http_status_code: int = None,
    row_count: int = None,
) -> None:
    """
    Writes a single row to ops.pipeline_run_log.
    Assumes ops.pipeline_run_log already exists.
    """
    sql = """
    INSERT INTO ops.pipeline_run_log (
        pipeline_name,
        run_id,
        series_id,
        status,
        message,
        http_status_code,
        row_count,
        started_utc_dt,
        finished_utc_dt
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
    """
    cursor.execute(
        sql,
        pipeline_name,
        run_id,
        series_id,
        status,
        message,
        http_status_code,
        row_count,
    )


def fetch_fred_observations(series_id: str) -> tuple[dict, int, str]:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }
    response = requests.get(BASE_URL, params=params, timeout=60)
    source_url = response.url
    status_code = response.status_code
    response.raise_for_status()
    return response.json(), status_code, source_url


def insert_bronze_payload(
    cursor,
    run_id: str,
    series_id: str,
    source_url: str,
    http_status_code: int,
    api_observation_count: int,
    response_json_text: str,
) -> None:
    sql = """
    INSERT INTO bronze.fred_observation_raw (
        run_id,
        ingestion_ts_utc,
        series_id,
        source_url,
        http_status_code,
        api_observation_count,
        response_json
    )
    VALUES (?, SYSUTCDATETIME(), ?, ?, ?, ?, ?);
    """
    cursor.execute(
        sql,
        run_id,
        series_id,
        source_url,
        http_status_code,
        api_observation_count,
        response_json_text,
    )


def main() -> None:
    pipeline_name = "load_bronze_fred_raw"
    run_id = str(uuid.uuid4())

    print(f"Starting {pipeline_name}. run_id={run_id}")

    conn = pyodbc.connect(SQL_CONN_STR)
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        log_run(
            cursor=cursor,
            pipeline_name=pipeline_name,
            run_id=run_id,
            status="STARTED",
            message=f"Series count: {len(SERIES_IDS)}",
        )

        success_count = 0
        error_count = 0

        for series_id in SERIES_IDS:
            try:
                print(f"Fetching {series_id}...")
                payload, status_code, source_url = fetch_fred_observations(series_id)
                obs_count = len(payload.get("observations", []))

                insert_bronze_payload(
                    cursor=cursor,
                    run_id=run_id,
                    series_id=series_id,
                    source_url=source_url,
                    http_status_code=status_code,
                    api_observation_count=obs_count,
                    response_json_text=json.dumps(payload),
                )

                log_run(
                    cursor=cursor,
                    pipeline_name=pipeline_name,
                    run_id=run_id,
                    series_id=series_id,
                    status="SUCCESS",
                    message="Loaded observations into bronze.fred_observation_raw",
                    http_status_code=status_code,
                    row_count=obs_count,
                )
                success_count += 1
                print(f"  OK {series_id}: {obs_count} observations")

            except Exception as exc:
                error_count += 1
                err_text = str(exc)[:1900]
                log_run(
                    cursor=cursor,
                    pipeline_name=pipeline_name,
                    run_id=run_id,
                    series_id=series_id,
                    status="FAILED",
                    message=err_text,
                )
                print(f"  ERROR {series_id}: {exc}")

        final_status = "SUCCESS" if error_count == 0 else "FAILED"
        log_run(
            cursor=cursor,
            pipeline_name=pipeline_name,
            run_id=run_id,
            status=final_status,
            message=f"Completed. success_count={success_count}, error_count={error_count}",
            row_count=success_count,
        )

        conn.commit()
        print("\nBronze load complete.")
        print(f"run_id={run_id}")
        print(f"success_count={success_count}")
        print(f"error_count={error_count}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

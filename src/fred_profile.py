import os
import time
from typing import Dict, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")
if not API_KEY:
    raise ValueError("FRED_API_KEY not found in .env")

BASE_URL = "https://api.stlouisfed.org/fred"

# Updated set: removed T10Y2Y due to intermittent endpoint issues.
# We can compute spread later as DGS10 - DGS2 in SQL/Tableau.
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

MAX_RETRIES = 3
BACKOFF_SECONDS = 2


def mask_key(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 6:
        return "***"
    return f"{value[:4]}...{value[-2:]}"


def get_json_with_retry(endpoint: str, params: Dict) -> Dict:
    """
    Calls FRED endpoint with basic retry/backoff for transient failures.
    Raises on persistent failure.
    """
    url = f"{BASE_URL}/{endpoint}"

    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_exception = exc
            if attempt < MAX_RETRIES:
                sleep_for = BACKOFF_SECONDS * attempt
                print(
                    f"Request failed (attempt {attempt}/{MAX_RETRIES}) "
                    f"for endpoint '{endpoint}', sleeping {sleep_for}s..."
                )
                time.sleep(sleep_for)
            else:
                break

    raise last_exception


def get_series_metadata(series_id: str) -> Dict:
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
    }
    payload = get_json_with_retry("series", params)

    # FRED really uses 'seriess' for this endpoint.
    series_list = payload.get("seriess", [])
    if not series_list:
        raise ValueError(f"No metadata returned for {series_id}")

    s = series_list[0]
    return {
        "series_id": series_id,
        "metric_name": s.get("title"),
        "native_frequency": s.get("frequency_short") or s.get("frequency"),
        "units": s.get("units"),
        "seasonal_adjustment": s.get("seasonal_adjustment_short") or s.get("seasonal_adjustment"),
    }


def get_series_observations(series_id: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
    }
    payload = get_json_with_retry("series/observations", params)

    observations = payload.get("observations", [])
    df = pd.DataFrame(observations)

    if df.empty:
        return pd.DataFrame(columns=["date", "value", "is_missing", "value_num"])

    df = df[["date", "value"]].copy()
    df["is_missing"] = df["value"].eq(".")
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def profile_series(series_id: str) -> Dict:
    meta = get_series_metadata(series_id)
    obs_df = get_series_observations(series_id)

    if obs_df.empty:
        return {
            **meta,
            "start_date": None,
            "end_date": None,
            "obs_count": 0,
            "missing_count": 0,
            "missing_pct": 0.0,
        }

    obs_count = int(len(obs_df))
    missing_count = int(obs_df["is_missing"].sum())
    missing_pct = round((missing_count / obs_count) * 100, 4) if obs_count else 0.0

    return {
        **meta,
        "start_date": obs_df["date"].min(),
        "end_date": obs_df["date"].max(),
        "obs_count": obs_count,
        "missing_count": missing_count,
        "missing_pct": missing_pct,
    }


def run_profiles(series_ids: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    success_rows = []
    error_rows = []

    for sid in series_ids:
        print(f"Profiling {sid}...")
        try:
            row = profile_series(sid)
            success_rows.append(row)
            print(f"  OK: {sid}")
        except Exception as exc:
            error_rows.append(
                {
                    "series_id": sid,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            print(f"  ERROR: {sid} -> {exc}")

    success_df = pd.DataFrame(success_rows)
    error_df = pd.DataFrame(error_rows)

    if not success_df.empty:
        success_df = success_df.sort_values("series_id").reset_index(drop=True)

    if not error_df.empty:
        error_df = error_df.sort_values("series_id").reset_index(drop=True)

    return success_df, error_df


def main() -> None:
    print(f"Starting FRED Phase 0 profile. API key detected: {mask_key(API_KEY)}")

    success_df, error_df = run_profiles(SERIES_IDS)

    os.makedirs("data", exist_ok=True)

    success_path = "data/phase0_series_profile.csv"
    error_path = "data/phase0_series_profile_errors.csv"

    success_df.to_csv(success_path, index=False)
    error_df.to_csv(error_path, index=False)

    print("\nRun complete.")
    print(f"Success rows: {len(success_df)}")
    print(f"Error rows: {len(error_df)}")
    print(f"Wrote: {success_path}")
    print(f"Wrote: {error_path}")

    if not success_df.empty:
        print("\nSuccess preview:")
        print(success_df.head(10).to_string(index=False))

    if not error_df.empty:
        print("\nErrors:")
        print(error_df.to_string(index=False))


if __name__ == "__main__":
    main()
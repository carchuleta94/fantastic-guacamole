--------------------------------------------------
-- Snapshots
--------------------------------------------------

SELECT TOP 24 *
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
ORDER BY month_key DESC;

SELECT TOP 20 *
FROM fantastic_guacamole.ops.pipeline_run_log
WHERE pipeline_name IN ('build_gold_consumer_finance_monthly','refresh_dim_date_daily')
ORDER BY run_log_id DESC;

--------------------------------------------------
-- silver to Gold check
--------------------------------------------------

-- 1)
-- Example: DGS10 monthly avg should match gold for that month_key
DECLARE @month_end date = '2026-04-30';

SELECT AVG(CAST(observation_value AS FLOAT)) AS silver_avg
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id = 'DGS10'
  AND EOMONTH(observation_date) = @month_end
  AND is_missing = 0
  AND observation_value IS NOT NULL;

SELECT dgs10_avg
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
WHERE month_key = CONVERT(int, FORMAT(@month_end, 'yyyyMMdd'));

-- 2)

SELECT TOP 50
    month_key,
    dgs10_avg,
    dgs2_avg,
    t10y2y_avg,
    (dgs10_avg - dgs2_avg) AS expected_t10y2y
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
WHERE dgs10_avg IS NOT NULL AND dgs2_avg IS NOT NULL
ORDER BY month_key DESC;

-- 3)

DECLARE @month_end date = '2026-03-31';
SELECT COUNT(*) AS silver_points_in_month
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id = 'UNRATE'
  AND EOMONTH(observation_date) = @month_end;

-- 4)

    -- A) List months where delinquency is present
    SELECT
        month_key,
        cc_delinquency_rate
    FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
    WHERE cc_delinquency_rate IS NOT NULL
    ORDER BY month_key DESC;

    -- B) Flag rows where delinquency exists but month-end is NOT a quarter-end month
    SELECT
        f.month_key,
        d.calendar_date,
        d.quarter_num,
        d.month_num,
        f.cc_delinquency_rate
    FROM fantastic_guacamole.gold.fact_consumer_finance_monthly f
    JOIN fantastic_guacamole.gold.dim_date d
      ON d.date_key = f.month_key
    WHERE f.cc_delinquency_rate IS NOT NULL
      AND d.month_num NOT IN (3, 6, 9, 12);  -- quarter-end months (calendar quarters)

    -- C) Silver-side truth check for one quarter-end month you pick
    DECLARE @q_end date = '2025-10-31';

    SELECT
        observation_date,
        observation_value
    FROM fantastic_guacamole.silver.fred_observation
    WHERE series_id = 'DRCCLACBS'
      AND EOMONTH(observation_date) = @q_end
      AND is_missing = 0
      AND observation_value IS NOT NULL
    ORDER BY observation_date DESC;

-- 5)

WITH m AS (
    SELECT
        month_key,
        dgs10_avg,
        dgs2_avg,
        dff_avg,
        sofr_avg,
        cc_balance_avg,
        consumer_credit_total,
        unemployment_rate,
        cc_delinquency_rate
    FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
)
SELECT TOP 50
    month_key,
    CASE WHEN dgs10_avg IS NULL THEN 0 ELSE 1 END AS has_rates,
    CASE WHEN consumer_credit_total IS NULL THEN 0 ELSE 1 END AS has_total_sl,
    CASE WHEN unemployment_rate IS NULL THEN 0 ELSE 1 END AS has_unrate,
    CASE WHEN cc_delinquency_rate IS NULL THEN 0 ELSE 1 END AS has_delinq
FROM m
WHERE (dgs10_avg IS NOT NULL OR dff_avg IS NOT NULL)
  AND (consumer_credit_total IS NULL OR unemployment_rate IS NULL OR cc_delinquency_rate IS NULL)
ORDER BY month_key DESC;

-- 6) re-run safety/idempotence check


SELECT COUNT(*) AS fact_rows_before
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly;

SELECT TOP 5 *
FROM fantastic_guacamole.ops.pipeline_run_log
WHERE pipeline_name = 'build_gold_consumer_finance_monthly'
ORDER BY run_log_id DESC;
    -- rows 1001
    -- 
EXEC fantastic_guacamole.ops.usp_build_gold_consumer_finance_monthly;
-- check after re-runs
SELECT COUNT(*) AS fact_rows_after
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly;

SELECT TOP 10 *
FROM fantastic_guacamole.ops.pipeline_run_log
WHERE pipeline_name = 'build_gold_consumer_finance_monthly'
ORDER BY run_log_id DESC;

/* C) Optional: show months whose load timestamp changed on second run */
SELECT month_key, load_ts_utc
FROM fantastic_guacamole.gold.fact_consumer_finance_monthly
ORDER BY load_ts_utc DESC;
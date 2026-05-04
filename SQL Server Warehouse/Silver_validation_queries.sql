---------------------------------------------
-- Snapshot Queries
---------------------------------------------

SELECT * FROM fantastic_guacamole.silver.fred_observation

SELECT * FROM fantastic_guacamole.silver.fred_series

SELECT TOP 50 * FROM ops.silver_load_tracker ORDER BY silver_load_tracker_id DESC;

SELECT TOP 50 * FROM ops.pipeline_run_log ORDER BY run_log_id DESC;

SELECT series_id, COUNT(*) AS row_count
FROM silver.fred_observation
GROUP BY series_id
ORDER BY series_id;

---------------------------------------------
-- date range checks
---------------------------------------------

SELECT
	s.series_id,
	s.series_name,
	MIN(o.observation_date) AS min_observation,
	MAX(o.observation_date) AS max_observation
FROM fantastic_guacamole.silver.fred_observation o
INNER JOIN fantastic_guacamole.silver.fred_series s
	ON s.series_id = o.series_id
GROUP BY s.series_id,s.series_name;

SELECT
	o.series_id,
	s.series_name,
	o.observation_date,
	COUNT(1) c
FROM fantastic_guacamole.silver.fred_observation o 
INNER JOIN fantastic_guacamole.silver.fred_series s
	ON s.series_id = o.series_id
GROUP BY o.series_id,s.series_name,o.observation_date
HAVING COUNT(1) > 1

SELECT * FROM fantastic_guacamole.silver.fred_observation WHERE observation_date IS NULL;
SELECT * FROM fantastic_guacamole.silver.fred_observation WHERE observation_date IS NULL;


---------------------------------------------
-- Missing values check
---------------------------------------------

SELECT COUNT(*) AS suspicious_missing_value_mismatch
FROM fantastic_guacamole.silver.fred_observation
WHERE is_missing = 0 AND observation_value IS NULL;
-- Any null dates (should be 0)
SELECT COUNT(*) AS null_dates
FROM fantastic_guacamole.silver.fred_observation
WHERE observation_date IS NULL;
-- Missing rate by series
SELECT
  series_id,
  SUM(CASE WHEN is_missing = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS missing_rate
FROM fantastic_guacamole.silver.fred_observation
GROUP BY series_id
ORDER BY series_id;

---------------------------------------------
-- spot checks
---------------------------------------------

-- 1) Check monthly avg against last trading day

DECLARE @y INT = 2026;
DECLARE @m INT = 4; -- April

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


-- 2) weekly cc balances

DECLARE @y INT = 2026;
DECLARE @m INT = 4;

DECLARE @month_start date = DATEFROMPARTS(@y, @m, 1);
DECLARE @month_end   date = EOMONTH(@month_start);

SELECT
    observation_date,
    observation_value
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id = 'CCLACBW027SBOG'
  AND observation_date BETWEEN @month_start AND @month_end
  AND is_missing = 0
  AND observation_value IS NOT NULL
ORDER BY observation_date;

-- 3) monthly series

DECLARE @y INT = 2026;
DECLARE @m INT = 4;

DECLARE @month_start date = DATEFROMPARTS(@y, @m, 1);
DECLARE @month_end   date = EOMONTH(@month_start);

SELECT series_id, observation_date, observation_value
FROM fantastic_guacamole.silver.fred_observation
WHERE series_id IN ('UNRATE','TOTALSL')
  AND observation_date BETWEEN @month_start AND @month_end
  AND is_missing = 0
  AND observation_value IS NOT NULL
ORDER BY series_id, observation_date;

-----------------------------------------------------

SELECT
    series_id,
    MAX(observation_date) AS latest_observation_date
FROM silver.fred_observation
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
  AND observation_date >= '2026-02-01'
  AND observation_date <  '2026-03-01'
  AND is_missing = 0
  AND observation_value IS NOT NULL
ORDER BY series_id, observation_date;

SELECT
    series_id,
    MAX(observation_date) AS latest_observation_date
FROM fantastic_guacamole.silver.fred_observation
WHERE is_missing = 0
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


	
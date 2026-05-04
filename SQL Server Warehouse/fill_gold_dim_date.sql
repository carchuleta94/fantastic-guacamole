USE fantastic_guacamole;
GO

;WITH bounds AS (
    SELECT
        MIN(EOMONTH(observation_date)) AS min_month_end,
        MAX(EOMONTH(observation_date)) AS max_month_end
    FROM fantastic_guacamole.silver.fred_observation
),
date_spine AS (
    SELECT DATEADD(DAY, -365, CAST(min_month_end AS DATE)) AS d
    FROM bounds
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM date_spine
    CROSS JOIN bounds
    WHERE d < DATEADD(DAY, 365, CAST(max_month_end AS DATE))
)
INSERT INTO fantastic_guacamole.gold.dim_date (
    date_key,
    calendar_date,
    year_num,
    quarter_num,
    month_num,
    month_name,
    week_start_date,
    is_month_end
)
SELECT
    CONVERT(INT, FORMAT(d, 'yyyyMMdd')) AS date_key,
    d AS calendar_date,
    YEAR(d) AS year_num,
    DATEPART(QUARTER, d) AS quarter_num,
    MONTH(d) AS month_num,
    DATENAME(MONTH, d) AS month_name,
    DATEADD(DAY, 1 - DATEPART(WEEKDAY, d), d) AS week_start_date,
    CASE WHEN d = EOMONTH(d) THEN 1 ELSE 0 END AS is_month_end
FROM date_spine
WHERE NOT EXISTS (
    SELECT 1
    FROM fantastic_guacamole.gold.dim_date x
    WHERE x.calendar_date = date_spine.d
)
OPTION (MAXRECURSION 32767);
GO
USE fantastic_guacamole;
GO

CREATE OR ALTER PROCEDURE ops.usp_build_gold_consumer_finance_monthly
    @pipeline_name VARCHAR(100) = 'build_gold_consumer_finance_monthly'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @rows_staged INT = 0;
    DECLARE @rows_affected INT = 0;

    BEGIN TRY
        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'STARTED',
            'Gold monthly build started', NULL, NULL, SYSUTCDATETIME(), NULL
        );

        IF OBJECT_ID('tempdb..#gold_monthly') IS NOT NULL DROP TABLE #gold_monthly;

        ;WITH silver_base AS (
            SELECT
                o.series_id,
                o.observation_date,
                o.observation_value,
                EOMONTH(o.observation_date) AS month_end_date
            FROM fantastic_guacamole.silver.fred_observation o
            WHERE o.is_missing = 0
              AND o.observation_value IS NOT NULL
        ),
        daily_weekly_rollup AS (
            SELECT
                month_end_date,
                AVG(CASE WHEN series_id = 'DGS10' THEN CAST(observation_value AS FLOAT) END) AS dgs10_avg,
                AVG(CASE WHEN series_id = 'DGS2' THEN CAST(observation_value AS FLOAT) END) AS dgs2_avg,
                AVG(CASE WHEN series_id = 'DFF' THEN CAST(observation_value AS FLOAT) END) AS dff_avg,
                AVG(CASE WHEN series_id = 'SOFR' THEN CAST(observation_value AS FLOAT) END) AS sofr_avg,
                AVG(CASE WHEN series_id = 'CCLACBW027SBOG' THEN CAST(observation_value AS FLOAT) END) AS cc_balance_avg
            FROM silver_base
            WHERE series_id IN ('DGS10','DGS2','DFF','SOFR','CCLACBW027SBOG')
            GROUP BY month_end_date
        ),
        monthly_rollup AS (
            SELECT
                month_end_date,
                MAX(CASE WHEN series_id = 'TOTALSL' THEN CAST(observation_value AS FLOAT) END) AS consumer_credit_total,
                MAX(CASE WHEN series_id = 'UNRATE' THEN CAST(observation_value AS FLOAT) END) AS unemployment_rate
            FROM silver_base
            WHERE series_id IN ('TOTALSL','UNRATE')
            GROUP BY month_end_date
        ),
        quarterly_rollup AS (
            SELECT
                EOMONTH(observation_date) AS month_end_date,
                MAX(CASE WHEN series_id = 'DRCCLACBS' THEN CAST(observation_value AS FLOAT) END) AS cc_delinquency_rate
            FROM silver_base
            WHERE series_id = 'DRCCLACBS'
            GROUP BY EOMONTH(observation_date)
        ),
        all_months AS (
            SELECT DISTINCT month_end_date FROM silver_base
        )
        SELECT
            CONVERT(INT, FORMAT(m.month_end_date, 'yyyyMMdd')) AS month_key,
            CAST(dw.dgs10_avg AS DECIMAL(18,6)) AS dgs10_avg,
            CAST(dw.dgs2_avg AS DECIMAL(18,6)) AS dgs2_avg,
            CAST(
                CASE
                    WHEN dw.dgs10_avg IS NULL OR dw.dgs2_avg IS NULL THEN NULL
                    ELSE dw.dgs10_avg - dw.dgs2_avg
                END
            AS DECIMAL(18,6)) AS t10y2y_avg,
            CAST(dw.dff_avg AS DECIMAL(18,6)) AS dff_avg,
            CAST(dw.sofr_avg AS DECIMAL(18,6)) AS sofr_avg,
            CAST(dw.cc_balance_avg AS DECIMAL(18,2)) AS cc_balance_avg,
            CAST(mo.consumer_credit_total AS DECIMAL(18,2)) AS consumer_credit_total,
            CAST(mo.unemployment_rate AS DECIMAL(10,4)) AS unemployment_rate,
            CAST(q.cc_delinquency_rate AS DECIMAL(10,4)) AS cc_delinquency_rate,
            CAST(NULL AS DECIMAL(10,4)) AS stress_index
        INTO #gold_monthly
        FROM all_months m
        LEFT JOIN daily_weekly_rollup dw ON dw.month_end_date = m.month_end_date
        LEFT JOIN monthly_rollup mo ON mo.month_end_date = m.month_end_date
        LEFT JOIN quarterly_rollup q ON q.month_end_date = m.month_end_date;

        SELECT @rows_staged = COUNT(*) FROM #gold_monthly;

        MERGE fantastic_guacamole.gold.fact_consumer_finance_monthly AS tgt
        USING #gold_monthly AS src
          ON tgt.month_key = src.month_key
        WHEN MATCHED THEN UPDATE SET
            tgt.dgs10_avg = src.dgs10_avg,
            tgt.dgs2_avg = src.dgs2_avg,
            tgt.t10y2y_avg = src.t10y2y_avg,
            tgt.dff_avg = src.dff_avg,
            tgt.sofr_avg = src.sofr_avg,
            tgt.cc_balance_avg = src.cc_balance_avg,
            tgt.consumer_credit_total = src.consumer_credit_total,
            tgt.unemployment_rate = src.unemployment_rate,
            tgt.cc_delinquency_rate = src.cc_delinquency_rate,
            tgt.stress_index = src.stress_index,
            tgt.load_ts_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                month_key, dgs10_avg, dgs2_avg, t10y2y_avg, dff_avg, sofr_avg,
                cc_balance_avg, consumer_credit_total, unemployment_rate, cc_delinquency_rate,
                stress_index, load_ts_utc
            )
            VALUES (
                src.month_key, src.dgs10_avg, src.dgs2_avg, src.t10y2y_avg, src.dff_avg, src.sofr_avg,
                src.cc_balance_avg, src.consumer_credit_total, src.unemployment_rate, src.cc_delinquency_rate,
                src.stress_index, SYSUTCDATETIME()
            );

        SET @rows_affected = @@ROWCOUNT;

        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'SUCCESS',
            CONCAT('Gold build complete. staged=', @rows_staged, ', merge_affected=', @rows_affected),
            NULL, @rows_staged, SYSUTCDATETIME(), SYSUTCDATETIME()
        );
    END TRY
    BEGIN CATCH
        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'FAILED',
            LEFT(ERROR_MESSAGE(), 1900),
            NULL, NULL, SYSUTCDATETIME(), SYSUTCDATETIME()
        );
        THROW;
    END CATCH
END;
GO
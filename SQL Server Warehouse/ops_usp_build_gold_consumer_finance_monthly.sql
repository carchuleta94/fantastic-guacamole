USE fantastic_guacamole;
GO

CREATE OR ALTER PROCEDURE ops.usp_build_gold_consumer_finance_monthly
    @pipeline_name VARCHAR(100) = 'build_gold_consumer_finance_monthly'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @run_started_utc DATETIME2(0) = SYSUTCDATETIME();
    DECLARE @run_finished_utc DATETIME2(0);
    DECLARE @rows_staged INT = 0;
    DECLARE @rows_affected INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated INT = 0;

    BEGIN TRY
        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'STARTED',
            'Gold monthly build started', NULL, NULL, @run_started_utc, NULL
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

        /* CTE/stats approach for z-scores (maintainability-first) */
        IF OBJECT_ID('tempdb..#gold_scored') IS NOT NULL DROP TABLE #gold_scored;

        ;WITH stats AS (
            SELECT
                AVG(CAST(unemployment_rate AS FLOAT)) AS mean_unemployment,
                STDEV(CAST(unemployment_rate AS FLOAT)) AS stdev_unemployment,

                AVG(CAST(cc_delinquency_rate AS FLOAT)) AS mean_delinquency,
                STDEV(CAST(cc_delinquency_rate AS FLOAT)) AS stdev_delinquency,

                AVG(CAST(dff_avg AS FLOAT)) AS mean_dff,
                STDEV(CAST(dff_avg AS FLOAT)) AS stdev_dff,

                AVG(CAST(t10y2y_avg AS FLOAT)) AS mean_t10y2y,
                STDEV(CAST(t10y2y_avg AS FLOAT)) AS stdev_t10y2y
            FROM #gold_monthly
        ),
        z AS (
            SELECT
                g.month_key,
                g.dgs10_avg,
                g.dgs2_avg,
                g.t10y2y_avg,
                g.dff_avg,
                g.sofr_avg,
                g.cc_balance_avg,
                g.consumer_credit_total,
                g.unemployment_rate,
                g.cc_delinquency_rate,

                CASE
                    WHEN s.stdev_unemployment IS NULL OR s.stdev_unemployment = 0 OR g.unemployment_rate IS NULL
                    THEN NULL
                    ELSE (CAST(g.unemployment_rate AS FLOAT) - s.mean_unemployment) / s.stdev_unemployment
                END AS z_unemployment,

                CASE
                    WHEN s.stdev_delinquency IS NULL OR s.stdev_delinquency = 0 OR g.cc_delinquency_rate IS NULL
                    THEN NULL
                    ELSE (CAST(g.cc_delinquency_rate AS FLOAT) - s.mean_delinquency) / s.stdev_delinquency
                END AS z_delinquency,

                CASE
                    WHEN s.stdev_dff IS NULL OR s.stdev_dff = 0 OR g.dff_avg IS NULL
                    THEN NULL
                    ELSE (CAST(g.dff_avg AS FLOAT) - s.mean_dff) / s.stdev_dff
                END AS z_dff,

                CASE
                    WHEN s.stdev_t10y2y IS NULL OR s.stdev_t10y2y = 0 OR g.t10y2y_avg IS NULL
                    THEN NULL
                    ELSE (CAST(g.t10y2y_avg AS FLOAT) - s.mean_t10y2y) / s.stdev_t10y2y
                END AS z_t10y2y
            FROM #gold_monthly g
            CROSS JOIN stats s
        )
        SELECT
            z.month_key,
            z.dgs10_avg,
            z.dgs2_avg,
            z.t10y2y_avg,
            z.dff_avg,
            z.sofr_avg,
            z.cc_balance_avg,
            z.consumer_credit_total,
            z.unemployment_rate,
            z.cc_delinquency_rate,
            CAST(
                (
                    COALESCE(z.z_unemployment, 0.0) +
                    COALESCE(z.z_delinquency, 0.0) +
                    COALESCE(z.z_dff, 0.0) +
                    COALESCE(-1.0 * z.z_t10y2y, 0.0)
                )
                /
                NULLIF(
                    (CASE WHEN z.z_unemployment IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN z.z_delinquency IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN z.z_dff IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN z.z_t10y2y IS NOT NULL THEN 1 ELSE 0 END),
                    0
                )
            AS DECIMAL(10,4)) AS stress_index
        INTO #gold_scored
        FROM z;

        IF OBJECT_ID('tempdb..#merge_actions') IS NOT NULL DROP TABLE #merge_actions;
        CREATE TABLE #merge_actions (action_name NVARCHAR(10) NOT NULL);

        MERGE fantastic_guacamole.gold.fact_consumer_finance_monthly AS tgt
        USING #gold_scored AS src
          ON tgt.month_key = src.month_key
        WHEN MATCHED AND (
               ISNULL(tgt.dgs10_avg, -999999.0)             <> ISNULL(src.dgs10_avg, -999999.0)
            OR ISNULL(tgt.dgs2_avg, -999999.0)              <> ISNULL(src.dgs2_avg, -999999.0)
            OR ISNULL(tgt.t10y2y_avg, -999999.0)            <> ISNULL(src.t10y2y_avg, -999999.0)
            OR ISNULL(tgt.dff_avg, -999999.0)               <> ISNULL(src.dff_avg, -999999.0)
            OR ISNULL(tgt.sofr_avg, -999999.0)              <> ISNULL(src.sofr_avg, -999999.0)
            OR ISNULL(tgt.cc_balance_avg, -999999.0)        <> ISNULL(src.cc_balance_avg, -999999.0)
            OR ISNULL(tgt.consumer_credit_total, -999999.0) <> ISNULL(src.consumer_credit_total, -999999.0)
            OR ISNULL(tgt.unemployment_rate, -999999.0)     <> ISNULL(src.unemployment_rate, -999999.0)
            OR ISNULL(tgt.cc_delinquency_rate, -999999.0)   <> ISNULL(src.cc_delinquency_rate, -999999.0)
            OR ISNULL(tgt.stress_index, -999999.0)          <> ISNULL(src.stress_index, -999999.0)
        )
        THEN UPDATE SET
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
            tgt.load_ts_utc = @run_started_utc
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                month_key, dgs10_avg, dgs2_avg, t10y2y_avg, dff_avg, sofr_avg,
                cc_balance_avg, consumer_credit_total, unemployment_rate, cc_delinquency_rate,
                stress_index, load_ts_utc
            )
            VALUES (
                src.month_key, src.dgs10_avg, src.dgs2_avg, src.t10y2y_avg, src.dff_avg, src.sofr_avg,
                src.cc_balance_avg, src.consumer_credit_total, src.unemployment_rate, src.cc_delinquency_rate,
                src.stress_index, @run_started_utc
            )
        OUTPUT $action INTO #merge_actions(action_name);

        SELECT
            @rows_inserted = SUM(CASE WHEN action_name = 'INSERT' THEN 1 ELSE 0 END),
            @rows_updated  = SUM(CASE WHEN action_name = 'UPDATE' THEN 1 ELSE 0 END)
        FROM #merge_actions;

        SET @rows_inserted = ISNULL(@rows_inserted, 0);
        SET @rows_updated = ISNULL(@rows_updated, 0);
        SET @rows_affected = @rows_inserted + @rows_updated;
        SET @run_finished_utc = SYSUTCDATETIME();

        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'SUCCESS',
            CONCAT('Gold build complete. staged=', @rows_staged, ', inserted=', @rows_inserted, ', updated=', @rows_updated),
            NULL, @rows_staged, @run_started_utc, @run_finished_utc
        );
    END TRY
    BEGIN CATCH
        SET @run_finished_utc = SYSUTCDATETIME();

        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message, http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'FAILED',
            LEFT(ERROR_MESSAGE(), 1900),
            NULL, NULL, @run_started_utc, @run_finished_utc
        );
        THROW;
    END CATCH
END;
GO
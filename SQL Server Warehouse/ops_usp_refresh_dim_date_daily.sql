USE fantastic_guacamole;
GO

CREATE OR ALTER PROCEDURE ops.usp_refresh_dim_date_daily
    @pipeline_name VARCHAR(100) = 'refresh_dim_date_daily',
    @lookback_days INT = 365,
    @lookahead_days INT = 730
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @start_date date;
    DECLARE @end_date date;
    DECLARE @inserted_rows INT = 0;

    BEGIN TRY
        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message,
            http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'STARTED',
            CONCAT('Dim date refresh started. lookback=', @lookback_days, ', lookahead=', @lookahead_days),
            NULL, NULL, SYSUTCDATETIME(), NULL
        );

        /* Anchor window around "today" */
        SET @start_date = DATEADD(DAY, -@lookback_days, CONVERT(date, SYSUTCDATETIME()));
        SET @end_date   = DATEADD(DAY,  @lookahead_days, CONVERT(date, SYSUTCDATETIME()));

        ;WITH n AS (
            SELECT TOP (DATEDIFF(DAY, @start_date, @end_date) + 1)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
            FROM sys.all_objects
        ),
        dates AS (
            SELECT DATEADD(DAY, n.n, @start_date) AS d
            FROM n
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
            CONVERT(INT, FORMAT(d.d, 'yyyyMMdd')) AS date_key,
            d.d AS calendar_date,
            YEAR(d.d) AS year_num,
            DATEPART(QUARTER, d.d) AS quarter_num,
            MONTH(d.d) AS month_num,
            DATENAME(MONTH, d.d) AS month_name,
            DATEADD(DAY, 1 - DATEPART(WEEKDAY, d.d), d.d) AS week_start_date,
            CASE WHEN d.d = EOMONTH(d.d) THEN 1 ELSE 0 END AS is_month_end
        FROM dates d
        WHERE NOT EXISTS (
            SELECT 1
            FROM fantastic_guacamole.gold.dim_date x
            WHERE x.calendar_date = d.d
        );

        SET @inserted_rows = @@ROWCOUNT;

        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message,
            http_status_code, row_count, started_utc_dt, finished_utc_dt
        )
        VALUES (
            @pipeline_name, @run_id, NULL, 'SUCCESS',
            CONCAT('Dim date refresh complete. inserted_rows=', @inserted_rows),
            NULL, @inserted_rows, SYSUTCDATETIME(), SYSUTCDATETIME()
        );
    END TRY
    BEGIN CATCH
        INSERT INTO fantastic_guacamole.ops.pipeline_run_log (
            pipeline_name, run_id, series_id, status, message,
            http_status_code, row_count, started_utc_dt, finished_utc_dt
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
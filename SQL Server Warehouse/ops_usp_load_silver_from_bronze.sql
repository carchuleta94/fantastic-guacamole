USE fantastic_guacamole;
GO

CREATE OR ALTER PROCEDURE ops.usp_load_silver_from_bronze
    @run_id UNIQUEIDENTIFIER = NULL,      -- optional: process one run only
    @batch_size INT = 1000,               -- observation-row batch size
    @pipeline_name VARCHAR(100) = 'load_silver_from_bronze'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @batch_size IS NULL OR @batch_size <= 0
        SET @batch_size = 1000;

    ------------------------------------------------------------
    -- 0) Series metadata mapping (for silver.fred_series upsert)
    --    (Using static mapping because bronze observations payload
    --     does not include full title/frequency metadata.)
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#series_meta') IS NOT NULL DROP TABLE #series_meta;
    CREATE TABLE #series_meta (
        series_id            VARCHAR(50) PRIMARY KEY,
        series_name          VARCHAR(255) NOT NULL,
        native_frequency     VARCHAR(20) NOT NULL,
        units                VARCHAR(100) NULL,
        seasonal_adjustment  VARCHAR(100) NULL,
        source_name          VARCHAR(100) NOT NULL
    );

    INSERT INTO #series_meta (series_id, series_name, native_frequency, units, seasonal_adjustment, source_name)
    VALUES
      ('DGS10',            '10-Year Treasury Constant Maturity Rate',                     'D', 'Percent',                'NSA', 'FRED'),
      ('DGS2',             '2-Year Treasury Constant Maturity Rate',                      'D', 'Percent',                'NSA', 'FRED'),
      ('DFF',              'Federal Funds Effective Rate',                                'D', 'Percent',                'NSA', 'FRED'),
      ('SOFR',             'Secured Overnight Financing Rate',                            'D', 'Percent',                'NSA', 'FRED'),
      ('CCLACBW027SBOG',   'Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks', 'W', 'Billions of U.S. Dollars', 'NSA', 'FRED'),
      ('TOTALSL',          'Total Consumer Credit Owned and Securitized',                 'M', 'Millions of U.S. Dollars','SA', 'FRED'),
      ('UNRATE',           'Unemployment Rate',                                            'M', 'Percent',                'SA',  'FRED'),
      ('DRCCLACBS',        'Delinquency Rate on Credit Card Loans, All Commercial Banks', 'Q', 'Percent',                'SA',  'FRED');

    ------------------------------------------------------------
    -- 1) Build run list to process
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#runs_to_process') IS NOT NULL DROP TABLE #runs_to_process;
    CREATE TABLE #runs_to_process (
        run_id UNIQUEIDENTIFIER PRIMARY KEY
    );

    IF @run_id IS NOT NULL
    BEGIN
        INSERT INTO #runs_to_process (run_id)
        SELECT DISTINCT b.run_id
        FROM bronze.fred_observation_raw b
        WHERE b.run_id = @run_id;
    END
    ELSE
    BEGIN
        INSERT INTO #runs_to_process (run_id)
        SELECT DISTINCT b.run_id
        FROM bronze.fred_observation_raw b
        WHERE NOT EXISTS (
            SELECT 1
            FROM ops.silver_load_tracker t
            WHERE t.run_id = b.run_id
              AND t.pipeline_name = @pipeline_name
              AND t.status = 'SUCCESS'
        );
    END

    IF NOT EXISTS (SELECT 1 FROM #runs_to_process)
    BEGIN
        PRINT 'No eligible runs to process.';
        RETURN;
    END

    ------------------------------------------------------------
    -- 2) Process each run_id
    ------------------------------------------------------------
    DECLARE @current_run_id UNIQUEIDENTIFIER;
    DECLARE @tracker_id BIGINT;
    DECLARE @total_rows_staged INT;
    DECLARE @total_rows_inserted INT;
    DECLARE @total_rows_updated INT;
    DECLARE @start_row INT;
    DECLARE @end_row INT;
    DECLARE @max_row INT;
    DECLARE @err NVARCHAR(2000);
    DECLARE @run_started_utc DATETIME2(0);
    DECLARE @run_finished_utc DATETIME2(0);

    DECLARE run_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT run_id
        FROM #runs_to_process
        ORDER BY run_id;

    OPEN run_cursor;
    FETCH NEXT FROM run_cursor INTO @current_run_id;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Initialize counters
            SET @total_rows_staged = 0;
            SET @total_rows_inserted = 0;
            SET @total_rows_updated = 0;
            SET @run_started_utc = SYSUTCDATETIME();

            -- START tracker row
            INSERT INTO ops.silver_load_tracker (
                run_id, pipeline_name, source_layer, target_layer, status,
                batch_size, total_rows_staged, total_rows_inserted, total_rows_updated,
                error_message, started_utc_dt, finished_utc_dt, created_utc_dt, modified_utc_dt
            )
            VALUES (
                @current_run_id, @pipeline_name, 'bronze', 'silver', 'STARTED',
                @batch_size, NULL, NULL, NULL,
                NULL, @run_started_utc, NULL, @run_started_utc, @run_started_utc
            );

            SET @tracker_id = SCOPE_IDENTITY();

            -- START pipeline log row
            INSERT INTO ops.pipeline_run_log (
                pipeline_name, run_id, series_id, status, message,
                http_status_code, row_count, started_utc_dt, finished_utc_dt
            )
            VALUES (
                @pipeline_name, @current_run_id, NULL, 'STARTED',
                'Silver load started for run_id',
                NULL, NULL, @run_started_utc, NULL
            );

            --------------------------------------------------------
            -- Upsert silver.fred_series for series present in run
            --------------------------------------------------------
            ;WITH run_series AS (
                SELECT DISTINCT b.series_id
                FROM bronze.fred_observation_raw b
                WHERE b.run_id = @current_run_id
            )
            MERGE silver.fred_series AS tgt
            USING (
                SELECT
                    rs.series_id,
                    COALESCE(sm.series_name, rs.series_id) AS series_name,
                    COALESCE(sm.native_frequency, 'U') AS native_frequency,
                    sm.units,
                    sm.seasonal_adjustment,
                    COALESCE(sm.source_name, 'FRED') AS source_name
                FROM run_series rs
                LEFT JOIN #series_meta sm
                    ON sm.series_id = rs.series_id
            ) AS src
            ON tgt.series_id = src.series_id
            WHEN MATCHED AND (
                ISNULL(tgt.series_name, '') <> ISNULL(src.series_name, '')
                OR ISNULL(tgt.native_frequency, '') <> ISNULL(src.native_frequency, '')
                OR ISNULL(tgt.units, '') <> ISNULL(src.units, '')
                OR ISNULL(tgt.seasonal_adjustment, '') <> ISNULL(src.seasonal_adjustment, '')
                OR ISNULL(tgt.source, '') <> ISNULL(src.source_name, '')
            )
            THEN
                UPDATE SET
                    tgt.series_name = src.series_name,
                    tgt.native_frequency = src.native_frequency,
                    tgt.units = src.units,
                    tgt.seasonal_adjustment = src.seasonal_adjustment,
                    tgt.source = src.source_name,
                    tgt.last_refreshed_ts_utc = @run_started_utc
            WHEN NOT MATCHED THEN
                INSERT (series_id, series_name, native_frequency, units, seasonal_adjustment, source, last_refreshed_ts_utc)
                VALUES (src.series_id, src.series_name, src.native_frequency, src.units, src.seasonal_adjustment, src.source_name, @run_started_utc);

            --------------------------------------------------------
            -- Parse bronze JSON into staging
            --------------------------------------------------------
            IF OBJECT_ID('tempdb..#stg_obs') IS NOT NULL DROP TABLE #stg_obs;
            CREATE TABLE #stg_obs (
                row_num            INT NOT NULL PRIMARY KEY,
                source_raw_id      BIGINT NOT NULL,
                source_run_id      UNIQUEIDENTIFIER NOT NULL,
                series_id          VARCHAR(50) NOT NULL,
                observation_date   DATE NOT NULL,
                observation_value  DECIMAL(18,6) NULL,
                is_missing         BIT NOT NULL,
                native_frequency   VARCHAR(20) NOT NULL
            );

            INSERT INTO #stg_obs (
                row_num, source_raw_id, source_run_id, series_id,
                observation_date, observation_value, is_missing, native_frequency
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY b.raw_id, j.observation_date) AS row_num,
                b.raw_id AS source_raw_id,
                b.run_id AS source_run_id,
                b.series_id,
                j.observation_date,
                CASE
                    WHEN j.observation_value_raw = '.' THEN NULL
                    ELSE TRY_CONVERT(DECIMAL(18,6), j.observation_value_raw)
                END AS observation_value,
                CASE
                    WHEN j.observation_value_raw = '.' THEN 1
                    ELSE 0
                END AS is_missing,
                COALESCE(sm.native_frequency, 'U') AS native_frequency
            FROM bronze.fred_observation_raw b
            CROSS APPLY OPENJSON(b.response_json, '$.observations')
                WITH (
                    observation_date DATE '$.date',
                    observation_value_raw VARCHAR(50) '$.value'
                ) j
            LEFT JOIN #series_meta sm
                ON sm.series_id = b.series_id
            WHERE b.run_id = @current_run_id
              AND j.observation_date IS NOT NULL;

            SELECT @total_rows_staged = COUNT(*) FROM #stg_obs;

            --------------------------------------------------------
            -- Batched upsert into silver.fred_observation
            --------------------------------------------------------
            SET @start_row = 1;
            SELECT @max_row = ISNULL(MAX(row_num), 0) FROM #stg_obs;

            WHILE @start_row <= @max_row
            BEGIN
                SET @end_row = @start_row + @batch_size - 1;

                IF OBJECT_ID('tempdb..#merge_actions') IS NOT NULL DROP TABLE #merge_actions;
                CREATE TABLE #merge_actions (action_name NVARCHAR(10));

                MERGE silver.fred_observation AS tgt
                USING (
                    SELECT
                        s.series_id,
                        s.observation_date,
                        s.observation_value,
                        s.is_missing,
                        s.native_frequency,
                        s.source_run_id,
                        s.source_raw_id
                    FROM #stg_obs s
                    WHERE s.row_num BETWEEN @start_row AND @end_row
                ) AS src
                ON tgt.series_id = src.series_id
                   AND tgt.observation_date = src.observation_date
                WHEN MATCHED AND (
                    ISNULL(tgt.observation_value, -999999.0) <> ISNULL(src.observation_value, -999999.0)
                    OR tgt.is_missing <> src.is_missing
                    OR ISNULL(tgt.native_frequency, '') <> ISNULL(src.native_frequency, '')
                    OR tgt.run_id <> src.source_run_id
                    OR ISNULL(tgt.source_raw_id, -1) <> ISNULL(src.source_raw_id, -1)
                )
                THEN UPDATE SET
                    tgt.observation_value = src.observation_value,
                    tgt.is_missing = src.is_missing,
                    tgt.native_frequency = src.native_frequency,
                    tgt.run_id = src.source_run_id,
                    tgt.source_raw_id = src.source_raw_id,
                    tgt.ingestion_ts_utc = @run_started_utc
                WHEN NOT MATCHED BY TARGET
                THEN INSERT (
                    series_id, observation_date, observation_value, is_missing,
                    native_frequency, run_id, source_raw_id, ingestion_ts_utc
                )
                VALUES (
                    src.series_id, src.observation_date, src.observation_value, src.is_missing,
                    src.native_frequency, src.source_run_id, src.source_raw_id, @run_started_utc
                )
                OUTPUT $action INTO #merge_actions(action_name);

                SELECT
                    @total_rows_inserted = @total_rows_inserted + SUM(CASE WHEN action_name = 'INSERT' THEN 1 ELSE 0 END),
                    @total_rows_updated  = @total_rows_updated  + SUM(CASE WHEN action_name = 'UPDATE' THEN 1 ELSE 0 END)
                FROM #merge_actions;

                SET @start_row = @end_row + 1;
            END

            --------------------------------------------------------
            -- Mark SUCCESS
            --------------------------------------------------------
            SET @run_finished_utc = SYSUTCDATETIME();

            UPDATE ops.silver_load_tracker
            SET
                status = 'SUCCESS',
                total_rows_staged = @total_rows_staged,
                total_rows_inserted = @total_rows_inserted,
                total_rows_updated = @total_rows_updated,
                error_message = NULL,
                finished_utc_dt = @run_finished_utc,
                modified_utc_dt = @run_finished_utc
            WHERE silver_load_tracker_id = @tracker_id;

            INSERT INTO ops.pipeline_run_log (
                pipeline_name, run_id, series_id, status, message,
                http_status_code, row_count, started_utc_dt, finished_utc_dt
            )
            VALUES (
                @pipeline_name, @current_run_id, NULL, 'SUCCESS',
                CONCAT(
                    'Silver load complete. staged=', @total_rows_staged,
                    ', inserted=', @total_rows_inserted,
                    ', updated=', @total_rows_updated
                ),
                NULL, @total_rows_staged, @run_started_utc, @run_finished_utc
            );
        END TRY
        BEGIN CATCH
            SET @err = LEFT(ERROR_MESSAGE(), 1900);
            SET @run_finished_utc = SYSUTCDATETIME();

            UPDATE ops.silver_load_tracker
            SET
                status = 'FAILED',
                error_message = @err,
                finished_utc_dt = @run_finished_utc,
                modified_utc_dt = @run_finished_utc
            WHERE silver_load_tracker_id = @tracker_id;

            INSERT INTO ops.pipeline_run_log (
                pipeline_name, run_id, series_id, status, message,
                http_status_code, row_count, started_utc_dt, finished_utc_dt
            )
            VALUES (
                @pipeline_name, @current_run_id, NULL, 'FAILED',
                @err, NULL, NULL, @run_started_utc, @run_finished_utc
            );
        END CATCH;

        FETCH NEXT FROM run_cursor INTO @current_run_id;
    END

    CLOSE run_cursor;
    DEALLOCATE run_cursor;
END;
GO
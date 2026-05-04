USE fantastic_guacamole;
GO

IF OBJECT_ID('ops.pipeline_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE ops.pipeline_run_log (
        run_log_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_name     VARCHAR(100) NOT NULL,
        run_id            UNIQUEIDENTIFIER NOT NULL,
        series_id         VARCHAR(50) NULL,
        status            VARCHAR(20) NOT NULL, -- STARTED / SUCCESS / FAILED
        message           NVARCHAR(2000) NULL,
        http_status_code  INT NULL,
        row_count         INT NULL,
        started_utc_dt    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_utc_dt   DATETIME2(0) NULL
    );
END
GO
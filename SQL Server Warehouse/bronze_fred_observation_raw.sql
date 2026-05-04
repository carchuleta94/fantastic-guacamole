USE fantastic_guacamole;
GO

IF OBJECT_ID('bronze.fred_observation_raw', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.fred_observation_raw (
        raw_id               BIGINT IDENTITY(1,1) NOT NULL,
        run_id               UNIQUEIDENTIFIER NOT NULL,
        ingestion_ts_utc     DATETIME2(0) NOT NULL CONSTRAINT DF_bronze_raw_ingestion_ts DEFAULT SYSUTCDATETIME(),
        series_id            VARCHAR(50) NOT NULL,
        source_url           VARCHAR(500) NULL,
        http_status_code     INT NULL,
        api_observation_count INT NULL,
        response_json        NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bronze_fred_observation_raw PRIMARY KEY CLUSTERED (raw_id)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_bronze_raw_series_ingestion'
      AND object_id = OBJECT_ID('bronze.fred_observation_raw')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bronze_raw_series_ingestion
        ON bronze.fred_observation_raw (series_id, ingestion_ts_utc DESC);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_bronze_raw_run_id'
      AND object_id = OBJECT_ID('bronze.fred_observation_raw')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_bronze_raw_run_id
        ON bronze.fred_observation_raw (run_id);
END;
GO

USE fantastic_guacamole;
GO

IF OBJECT_ID('silver.fred_observation', 'U') IS NULL
BEGIN
    CREATE TABLE silver.fred_observation (
        obs_id               BIGINT IDENTITY(1,1) NOT NULL,
        series_id            VARCHAR(50) NOT NULL,
        observation_date     DATE NOT NULL,
        observation_value    DECIMAL(18,6) NULL,
        is_missing           BIT NOT NULL CONSTRAINT DF_silver_obs_is_missing DEFAULT (0),
        native_frequency     VARCHAR(20) NOT NULL,
        run_id               UNIQUEIDENTIFIER NOT NULL,
        ingestion_ts_utc     DATETIME2(0) NOT NULL CONSTRAINT DF_silver_obs_ingestion_ts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_silver_fred_observation PRIMARY KEY CLUSTERED (obs_id),
        CONSTRAINT FK_silver_obs_series FOREIGN KEY (series_id) REFERENCES silver.fred_series(series_id),
        CONSTRAINT UQ_silver_obs_series_date UNIQUE (series_id, observation_date)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_silver_obs_date_series'
      AND object_id = OBJECT_ID('silver.fred_observation')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_silver_obs_date_series
        ON silver.fred_observation (observation_date, series_id)
        INCLUDE (observation_value, is_missing, native_frequency);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_silver_obs_run_id'
      AND object_id = OBJECT_ID('silver.fred_observation')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_silver_obs_run_id
        ON silver.fred_observation (run_id, series_id);
END;
GO

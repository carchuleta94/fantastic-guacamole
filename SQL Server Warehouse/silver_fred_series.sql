USE fantastic_guacamole;
GO

IF OBJECT_ID('silver.fred_series', 'U') IS NULL
BEGIN
    CREATE TABLE silver.fred_series (
        series_id               VARCHAR(50) NOT NULL,
        series_name             VARCHAR(255) NOT NULL,
        native_frequency        VARCHAR(20) NOT NULL,
        units                   VARCHAR(100) NULL,
        seasonal_adjustment     VARCHAR(100) NULL,
        source                  VARCHAR(100) NULL,
        last_refreshed_ts_utc   DATETIME2(0) NOT NULL CONSTRAINT DF_silver_series_last_refreshed DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_silver_fred_series PRIMARY KEY CLUSTERED (series_id)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_silver_series_frequency'
      AND object_id = OBJECT_ID('silver.fred_series')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_silver_series_frequency
        ON silver.fred_series (native_frequency, series_id);
END;
GO

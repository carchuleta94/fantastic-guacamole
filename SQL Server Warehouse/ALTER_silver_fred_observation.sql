USE fantastic_guacamole;
GO

IF COL_LENGTH('silver.fred_observation', 'source_raw_id') IS NULL
BEGIN
    ALTER TABLE silver.fred_observation
    ADD source_raw_id BIGINT NULL;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_silver_obs_source_raw_id'
      AND parent_object_id = OBJECT_ID('silver.fred_observation')
)
BEGIN
    ALTER TABLE silver.fred_observation
    ADD CONSTRAINT FK_silver_obs_source_raw_id
        FOREIGN KEY (source_raw_id)
        REFERENCES bronze.fred_observation_raw(raw_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_silver_obs_source_raw_id'
      AND object_id = OBJECT_ID('silver.fred_observation')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_silver_obs_source_raw_id
        ON silver.fred_observation (source_raw_id);
END;
GO
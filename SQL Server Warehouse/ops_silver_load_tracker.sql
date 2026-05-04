USE fantastic_guacamole;
GO

IF OBJECT_ID('ops.silver_load_tracker', 'U') IS NULL
BEGIN
    CREATE TABLE ops.silver_load_tracker (
        silver_load_tracker_id   BIGINT IDENTITY(1,1) NOT NULL,
        run_id                   UNIQUEIDENTIFIER NOT NULL,
        pipeline_name            VARCHAR(100) NOT NULL,   -- e.g. 'load_silver_from_bronze'
        source_layer             VARCHAR(20)  NOT NULL CONSTRAINT DF_slt_source_layer DEFAULT ('bronze'),
        target_layer             VARCHAR(20)  NOT NULL CONSTRAINT DF_slt_target_layer DEFAULT ('silver'),
        status                   VARCHAR(20)  NOT NULL,   -- STARTED / SUCCESS / FAILED / PARTIAL
        batch_size               INT          NOT NULL CONSTRAINT DF_slt_batch_size DEFAULT (1000),
        total_rows_staged        INT          NULL,
        total_rows_inserted      INT          NULL,
        total_rows_updated       INT          NULL,
        error_message            NVARCHAR(2000) NULL,
        started_utc_dt           DATETIME2(0) NOT NULL CONSTRAINT DF_slt_started_utc_dt DEFAULT SYSUTCDATETIME(),
        finished_utc_dt          DATETIME2(0) NULL,
        created_utc_dt           DATETIME2(0) NOT NULL CONSTRAINT DF_slt_created_utc_dt DEFAULT SYSUTCDATETIME(),
        modified_utc_dt          DATETIME2(0) NOT NULL CONSTRAINT DF_slt_modified_utc_dt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ops_silver_load_tracker PRIMARY KEY CLUSTERED (silver_load_tracker_id),
        CONSTRAINT CK_slt_status CHECK (status IN ('STARTED','SUCCESS','FAILED','PARTIAL'))
    );
END;
GO

/* Prevent duplicate successful completion rows for same run + pipeline */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_slt_run_pipeline_status'
      AND object_id = OBJECT_ID('ops.silver_load_tracker')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_slt_run_pipeline_status
        ON ops.silver_load_tracker (run_id, pipeline_name, status);
END;
GO

/* Fast lookup for "what runs are done" and resume logic */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_slt_pipeline_status_run'
      AND object_id = OBJECT_ID('ops.silver_load_tracker')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_slt_pipeline_status_run
        ON ops.silver_load_tracker (pipeline_name, status, run_id)
        INCLUDE (started_utc_dt, finished_utc_dt, total_rows_staged, total_rows_inserted, total_rows_updated);
END;
GO

/* Recent-run monitoring */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_slt_started_desc'
      AND object_id = OBJECT_ID('ops.silver_load_tracker')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_slt_started_desc
        ON ops.silver_load_tracker (started_utc_dt DESC)
        INCLUDE (pipeline_name, run_id, status, error_message);
END;
GO
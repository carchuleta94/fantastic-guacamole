USE fantastic_guacamole;
GO

IF OBJECT_ID('gold.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_date (
        date_key          INT NOT NULL,           -- YYYYMMDD
        calendar_date     DATE NOT NULL,
        year_num          SMALLINT NOT NULL,
        quarter_num       TINYINT NOT NULL,
        month_num         TINYINT NOT NULL,
        month_name        VARCHAR(15) NOT NULL,
        week_start_date   DATE NOT NULL,
        is_month_end      BIT NOT NULL,
        CONSTRAINT PK_gold_dim_date PRIMARY KEY CLUSTERED (date_key),
        CONSTRAINT UQ_gold_dim_date_calendar_date UNIQUE (calendar_date)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_gold_dim_date_month'
      AND object_id = OBJECT_ID('gold.dim_date')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_gold_dim_date_month
        ON gold.dim_date (year_num, month_num, calendar_date);
END;
GO

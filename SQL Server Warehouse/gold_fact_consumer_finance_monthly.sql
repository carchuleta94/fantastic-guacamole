USE fantastic_guacamole;
GO

IF OBJECT_ID('gold.fact_consumer_finance_monthly', 'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_consumer_finance_monthly (
        month_key              INT NOT NULL,  -- join to gold.dim_date.date_key (month-end date)
        dgs10_avg              DECIMAL(18,6) NULL,
        dgs2_avg               DECIMAL(18,6) NULL,
        t10y2y_avg             DECIMAL(18,6) NULL,
        dff_avg                DECIMAL(18,6) NULL,
        sofr_avg               DECIMAL(18,6) NULL,
        cc_balance_avg         DECIMAL(18,2) NULL,
        consumer_credit_total  DECIMAL(18,2) NULL,
        unemployment_rate      DECIMAL(10,4) NULL,
        cc_delinquency_rate    DECIMAL(10,4) NULL,
        stress_index           DECIMAL(10,4) NULL,
        load_ts_utc            DATETIME2(0) NOT NULL CONSTRAINT DF_gold_fact_load_ts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_gold_fact_consumer_finance_monthly PRIMARY KEY CLUSTERED (month_key),
        CONSTRAINT FK_gold_fact_month_key FOREIGN KEY (month_key) REFERENCES gold.dim_date(date_key)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_gold_fact_load_ts'
      AND object_id = OBJECT_ID('gold.fact_consumer_finance_monthly')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_gold_fact_load_ts
        ON gold.fact_consumer_finance_monthly (load_ts_utc DESC)
        INCLUDE (stress_index, unemployment_rate, cc_delinquency_rate);
END;
GO

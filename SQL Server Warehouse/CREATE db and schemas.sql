-----------------------------
-----------------------------
-- 1 ) Create DB
-----------------------------
-----------------------------

IF DB_ID('fantastic_guacamole') IS NULL
BEGIN
	CREATE DATABASE fantastic_guacamole;
END
GO

USE fantastic_guacamole;
GO

-----------------------------
-----------------------------
-- 2) Create Medallion Schemas
-----------------------------
-----------------------------

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
	EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
	EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
	EXEC('CREATE SCHEMA gold');
GO

IF NOT EXISTS (select 1 FROM sys.schemas WHERE name = 'ops')
	EXEC('CREATE SCHEMA ops');
GO
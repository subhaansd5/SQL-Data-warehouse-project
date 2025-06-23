/*
========================================================
Create Database and Schemas
========================================================

Script perpose:
	The script creates new database named 'Datawarehouse' after checking
	if it already exists. If the database exists then it will drop the database and will recreate it.
	Additionally, the script will create 3 schemas within the database
	named as 'bronze', 'silver', and 'gold'.

WARNING:
	Ececuting this script will drop the entire 'Datawarehous' Database if it exist.
	All data in databases will permanantly deleted. Proceed with caution and ensure
	to have complete data backup before running this script.

*/


USE master;
GO

--Drop and recreate the 'Datawarehouse' database.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Datawarehouse' )
BEGIN
		ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE Datawarehouse;
END;
GO

-- Create the 'Datawarehouse' database.
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

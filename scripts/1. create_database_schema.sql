/*
==============================================================
CREATING DATABASE AND SCHEMAS
==============================================================

1. Script Purpose:
   This script creates a new database called 'DataWarehouse' after checking if it already exists. 
   If it does, the database will be dropped and recreated. It also creates three schemas: bronze, silver, and gold.

2. Warning Message:
   Running this script will permanently delete the existing 'DataWarehouse' database and all its contents.
   Ensure you have a backup before proceeding.
*/

USE master;
GO

-- Drop 'DataWarehouse' if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
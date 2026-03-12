/*
===============================================================================
Script: Database and Schema Initialization
Project: Modern Data Warehouse using SQL Server
Author: Avantika Kumaresan
Description:
    This script initializes the foundational structure of the Data Warehouse.

    It performs the following steps:
    1. Checks if the DataWarehouse database already exists.
    2. If it exists, the database is switched to SINGLE_USER mode and dropped.
    3. Recreates the DataWarehouse database.
    4. Creates schemas to represent different layers of the data warehouse.

Schemas Created:
    bronze  -> Raw ingested data from source systems
    silver  -> Cleaned and transformed data
    gold    -> Business-ready analytical tables

This layered architecture follows the Medallion Architecture pattern
commonly used in modern data engineering pipelines.
Warning: If you run this script the already existing databse will be completely dropped.
===============================================================================
*/

-- Switch context to the master database to manage database-level operations
USE master;
GO

/*
Check if the DataWarehouse database already exists.
*/

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN

    /*
    If the database exists, switch it to SINGLE_USER mode.
    */

    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO


/*
Create a fresh DataWarehouse database.
This will store all layers of the modern data warehouse.
*/
CREATE DATABASE DataWarehouse;
GO


-- Switch context to the newly created DataWarehouse database
USE DataWarehouse;
GO


/*
Create the Bronze schema.

The Bronze layer stores raw data ingested directly from source systems
such as ERP or CRM without major transformations.
*/
CREATE SCHEMA bronze;
GO


/*
Create the Silver schema.

The Silver layer contains cleaned, standardized, and transformed data
that has been prepared for further analysis.
*/
CREATE SCHEMA silver;
GO


/*
Create the Gold schema.

The Gold layer contains business-ready datasets optimized for analytics,
reporting, and dashboarding.
*/
CREATE SCHEMA gold;
GO

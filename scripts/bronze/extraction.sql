/*
===============================================================================
Script: Bronze Layer Data Load Procedure
Project: Modern Data Warehouse (SQL Server)
Author: Avantika Kuamresan

Description:
This stored procedure loads raw data into the Bronze layer tables of the 
Data Warehouse using BULK INSERT.

Steps performed:
1. Truncate existing data in Bronze tables.
2. Load fresh data from CSV files (CRM and ERP sources).
3. Track and print load duration for each table.
4. Capture errors using TRY...CATCH for debugging.

This procedure represents the data ingestion stage of the ETL pipeline.
===============================================================================
*/

USE DataWarehouse;
GO


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

DECLARE 
    @start_time DATETIME,
    @end_time DATETIME,
    @batch_start_time DATETIME,
    @batch_end_time DATETIME;

BEGIN TRY

SET @batch_start_time = GETDATE();



/* ============================
   Load CRM Customer Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.crm_cst_info;

BULK INSERT bronze.crm_cst_info
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\cust_info.csv'
WITH (
	FIRSTROW = 2,        -- Skip header row
	FIELDTERMINATOR = ',', 
	TABLOCK              -- Improves bulk load performance
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* ============================
   Load CRM Product Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.crm_prod_info;

BULK INSERT bronze.crm_prod_info
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* ============================
   Load CRM Sales Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\sales_Details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* ============================
   Load ERP Customer Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\ERP\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* ============================
   Load ERP Location Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\ERP\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* ============================
   Load ERP Product Category Data
   ============================ */

SET @start_time = GETDATE();

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\avant\OneDrive\Desktop\Data engineering project\ERP\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
      + ' seconds';

PRINT '==========================================';



/* Total batch load duration */

SET @batch_end_time = GETDATE();

PRINT 'Total Load Duration: ' 
      + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
      + ' seconds';

END TRY



/* Error handling */

BEGIN CATCH

PRINT '==========================================';
PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
PRINT 'Error Message: ' + ERROR_MESSAGE();
PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '==========================================';

END CATCH

END;
GO


/* Execute the procedure */

EXEC bronze.load_bronze;

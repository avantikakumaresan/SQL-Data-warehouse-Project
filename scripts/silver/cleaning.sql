/*
===============================================================================
Script: Silver Layer Data Transformation Procedure
Project: Modern Data Warehouse (SQL Server)
Author: Avantika Kumaresan

Description:
This stored procedure loads and transforms data from the Bronze layer into
the Silver layer. The Silver layer contains cleaned, standardized, and
validated data ready for analytics processing.

Main operations performed:
1. Remove duplicates and clean customer data.
2. Standardize product attributes and derive category information.
3. Validate and correct sales transactions.
4. Clean ERP customer demographic data.
5. Standardize location and country information.
6. Load product category reference data.

The procedure also tracks execution time for each step and includes
error handling using TRY...CATCH.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

DECLARE 
    @start_time DATETIME,
    @end_time DATETIME,
    @batch_start_time DATETIME,
    @batch_end_time DATETIME;

BEGIN TRY

SET @batch_start_time = GETDATE();


/* =====================================================
   Load and Clean CRM Customer Information
   - Remove duplicates
   - Standardize gender and marital status
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.crm_cst_info;

INSERT INTO silver.crm_cst_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    -- Convert marital status codes to readable values
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END,

    -- Convert gender codes to readable values
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END,

    cst_create_date

FROM
(
    -- Deduplicate records keeping latest customer record
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY cst_id 
        ORDER BY cst_create_date DESC
    ) rank
    FROM bronze.crm_cst_info
    WHERE cst_id IS NOT NULL
) ranked

WHERE rank = 1;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Load and Transform CRM Product Information
   - Extract product key and category
   - Standardize product line names
   - Calculate product end date using LEAD()
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.crm_prod_info;

INSERT INTO silver.crm_prod_info
(
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT
    prd_id,

    -- Extract product key from source key
    SUBSTRING(prd_key,7,LEN(prd_key)),

    -- Derive category id
    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),

    prd_nm,

    -- Replace NULL cost with 0
    ISNULL(prd_cost,0),

    -- Map product line codes to descriptions
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Roads'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,

    CAST(prd_start_dt AS DATE),

    -- Determine end date using next product version
    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        ) - 1 AS DATE
    )

FROM bronze.crm_prod_info;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Load and Validate CRM Sales Transactions
   - Fix invalid dates
   - Recalculate incorrect sales values
   - Handle missing price data
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_price,
    sls_quantity
)

SELECT

sls_ord_num,
sls_prd_key,
sls_cust_id,

-- Validate order date
CASE
    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END,

-- Validate ship date
CASE
    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END,

-- Validate due date
CASE
    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END,

-- Fix incorrect sales calculation
CASE
    WHEN sls_sales <= 0 
    OR sls_sales IS NULL
    OR sls_sales <> sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END,

-- Fix invalid price values
CASE
    WHEN sls_price <= 0 OR sls_price IS NULL
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END,

sls_quantity

FROM bronze.crm_sales_details;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Clean ERP Customer Demographic Data
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12
(
CID,
BDATE,
GEN
)

SELECT
    
-- Extract customer key
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
     ELSE CID
     END CID,

-- Remove future birthdates
CASE
    WHEN BDATE > GETDATE() THEN NULL
    ELSE BDATE
END,

-- Standardize gender
CASE
    WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
END

FROM bronze.erp_cust_az12;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Clean ERP Location Data
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101
(
CID,
CNTRY
)

SELECT

-- Remove dashes from customer id
REPLACE(CID,'-',''),

-- Standardize country values
CASE
    WHEN UPPER(TRIM(CNTRY)) IN ('USA','UNITED STATES','US') THEN 'United States'
    WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
    WHEN UPPER(TRIM(CNTRY)) = '' OR CNTRY IS NULL THEN 'n/a'
    ELSE TRIM(CNTRY)
END

FROM bronze.erp_loc_a101;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Load Product Category Reference Data
   ===================================================== */

SET @start_time = GETDATE();

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
(
ID,
CAT,
SUBCAT,
MAINTENANCE
)

SELECT *
FROM bronze.erp_px_cat_g1v2;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
PRINT '==========================================';



/* =====================================================
   Total Batch Execution Time
   ===================================================== */

SET @batch_end_time = GETDATE();

PRINT 'Total Load Duration: ' 
+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) 
+ ' seconds';

END TRY



/* =====================================================
   Error Handling
   ===================================================== */

BEGIN CATCH

PRINT '==========================================';
PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
PRINT 'Error Message: ' + ERROR_MESSAGE();
PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '==========================================';

END CATCH

END
GO


/* Execute Silver Layer Load */

EXEC silver.load_silver;

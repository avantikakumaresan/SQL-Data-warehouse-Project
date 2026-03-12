/*
===============================================================================
Script: silver Layer Table Creation
Project: Modern Data Warehouse (SQL Server)
Author:Avantika Kumaresan

Description:
This script creates the silver layer tables in the Data Warehouse.
The silver layer stores the cleaned data. 
Once the table is created teh cleaning and transformation is done.

Steps performed:
1. Check if each table already exists.
2. Drop the table if it exists to avoid duplication.
3. Recreate the table structure for raw data ingestion.
===============================================================================
*/

USE DataWarehouse;
GO


/* CRM Customer Information Table */

IF OBJECT_ID('silver.crm_cst_info', 'U') IS NOT NULL
DROP TABLE silver.crm_cst_info;

CREATE TABLE silver.crm_cst_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);



/* CRM Product Information Table */

IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL
DROP TABLE silver.crm_prod_info;

CREATE TABLE silver.crm_prod_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	cat_id NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE() -- record load timestamp
);



/* CRM Sales Transactions Table */

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);



/* ERP Customer Demographic Data */

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
);



/* ERP Customer Location Data */

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);



/* ERP Product Category Information */

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
);

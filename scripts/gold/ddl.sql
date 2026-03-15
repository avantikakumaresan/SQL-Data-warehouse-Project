/*
===============================================================================
Script: Gold Layer Analytical Views
Project: Modern Data Warehouse (SQL Server)
Author: Avantika Kumaresan

Description:
This script creates the Gold layer views in the data warehouse.
The Gold layer contains business-ready datasets optimized for analytics,
reporting, and dashboarding.

The script creates:
1. dim_customers  → Customer dimension table
2. dim_products   → Product dimension table
3. fact_sales     → Sales fact table

These views transform the cleaned Silver layer data into a dimensional
model (Star Schema) used for analytical queries and BI reporting.
===============================================================================
*/

USE DataWarehouse;
GO


/* =====================================================
   Customer Dimension
   Combines CRM customer data with ERP demographic
   and location information.
   ===================================================== */

CREATE VIEW gold.dim_customers AS
SELECT 

    -- Generate surrogate key for the dimension table
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,

	-- Customer location
	lo.cntry AS country,

	ci.cst_marital_status AS marital_status,

	-- Prefer gender from CRM, otherwise use ERP data
	CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
	    ELSE COALESCE(ca.gen,'n/a')
	END AS gender,

	ca.BDATE AS birthdate,
	ci.cst_create_date AS create_date

FROM silver.crm_cst_info ci

-- Join ERP customer demographic information
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID

-- Join ERP location information
LEFT JOIN silver.erp_loc_a101 lo
ON ci.cst_key = lo.CID;



/*
validation query used during development
to compare gender values between CRM and ERP sources.

SELECT DISTINCT
ci.cst_gndr,
ca.GEN,
CASE
WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
ELSE COALESCE(ca.gen,'n/a')
END AS new_gen
FROM silver.crm_cst_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID;
*/



/* =====================================================
   Product Dimension
   Enriches product information with category data
   from ERP reference tables.
   ===================================================== */

CREATE VIEW gold.dim_products AS
SELECT 

    -- Generate surrogate key
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,

	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,

	pi.cat_id AS category_id,
	ca.cat AS category,
	ca.subcat AS subcategory,
	ca.MAINTENANCE,

	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date

FROM silver.crm_prod_info pi

-- Join ERP category reference data
LEFT JOIN silver.erp_px_cat_g1v2 ca
ON pi.cat_id = ca.ID

-- Keep only currently active products (exclude historical records)
WHERE pi.prd_end_dt IS NULL;



/* =====================================================
   Sales Fact Table
   Links customer and product dimensions with
   transactional sales data.
   ===================================================== */

CREATE VIEW gold.fact_sales AS
SELECT

	sa.sls_ord_num AS order_number,

	-- Foreign keys referencing dimension tables
	pr.product_key,
	cu.customer_key,

	sa.sls_order_dt AS order_date,
	sa.sls_ship_dt AS shipping_date,
	sa.sls_due_dt AS due_date,

	sa.sls_sales AS sales_amount,
	sa.sls_quantity AS quantity,
	sa.sls_price AS price

FROM silver.crm_sales_details sa

-- Join product dimension
LEFT JOIN gold.dim_products pr
ON sa.sls_prd_key = pr.product_number

-- Join customer dimension
LEFT JOIN gold.dim_customers cu
ON sa.sls_cust_id = cu.customer_id;





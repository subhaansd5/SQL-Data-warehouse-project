/*
================================================================
DDL Script : Create gold views
================================================================
Script Purpose : This script create views for the gold layer in data ware house.
                 And the gold layer represents the final dimension and fact tables(STAR schema).

				 Each view performs transeformation and combinded data from different tables in silver
				 layer to produce clean, enriched and business- ready data.


Usage: This views can directly queryid for analytics and reporting.
====================================================================


*/

--============================================================
--Creating dimension customer
--============================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;

GO 

CREATE VIEW gold.dim_customers AS

SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,		
	ci.cst_id AS customer_id,
	ci.cst_key AS custome_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ce.gen, 'n/a')
	END  AS gender,
	ce.bdate AS birthdate,
	ci.cst_create_date AS create_date		
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ce
ON ci.cst_key = ce.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid 

GO

--==========================================================
--Creating dimension product
--==========================================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
   DROP VIEW gold.dim_products;

GO

CREATE VIEW gold.dim_products AS

SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS sub_category,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
	
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data -- now we have only current data

GO

--=====================================================
--Creating fact sales
--=====================================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 

SELECT
	sd.sls_ord_num AS order_number,		
	pr.product_key,
	cs.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quanity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cs
ON sd.sls_cust_id =	cs.customer_id


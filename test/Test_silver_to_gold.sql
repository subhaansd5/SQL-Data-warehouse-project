
---##### Working on silver layer to transeform in gold layer

--business object : customer
--getting column names 
SELECT 'ci.'+name+','
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.crm_cust_info')

   
SELECT 'ce.'+name+','
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.erp_cust_az12')


SELECT 'cl.'+name+','
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.erp_loc_a101')
   
--Joined all table of customer details and then checked any dupliacte records
SELECT cst_id, COUNT(*)
FROM (
		SELECT 
			ci.cst_id,
			ci.cst_key,
			ci.cst_firstname,
			ci.cst_lastname,
			ci.cst_marital_status,
			ci.cst_gndr,
			ci.cst_create_date,
			ce.bdate,
			ce.gen,
			cl.cntry
		FROM silver.crm_cust_info AS ci
		LEFT JOIN silver.erp_cust_az12 AS ce
		ON ci.cst_key = ce.cid
		LEFT JOIN silver.erp_loc_a101 AS cl
		ON ci.cst_key = cl.cid 

)t GROUP BY cst_id
HAVING COUNT(*) > 1


--Handling Gender columns there two columns so taking details from primary table crm_cust_info
-- if gender details are not available in the primary table then taking details from the silver.erp_cust_az12


SELECT 
		DISTINCT ci.cst_gndr,
		ce.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ce
ON ci.cst_key = ce.cid
		
SELECT 
		DISTINCT ci.cst_gndr,
		ce.gen,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ce.gen, 'n/a')
		END new_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ce
ON ci.cst_key = ce.cid

SELECT * FROM gold.dim_customers


--business object : Product
--getting column names 
SELECT 'pn.'+name+','
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.crm_prd_info')

--- Joined tables of business object product table:silver.crm_prd_info and silver.erp_px_cat_g1v2 
--- then check duplicates

SELECT prd_key, COUNT(*)
FROM (
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
	
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data -- now we have only current data

) t
GROUP BY prd_key
HAVING COUNT(*) > 1

SELECT * FROM gold.dim_products


--business object : sales
--getting column names 
SELECT 'sd.'+name+','
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.crm_sales_details')

 SELECT name
 FROM sys.columns 
 WHERE object_id= OBJECT_ID('gold.fact_sales')

 --Foreign key integrity

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE c.customer_key IS NULL AND p.product_key IS NULL
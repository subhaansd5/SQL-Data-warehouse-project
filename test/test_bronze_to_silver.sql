--checked the duplicates and nulls for primary key cst_id

SELECT * FROM ( 
SELECT 
* ,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn_or_flag_last
FROM bronze.crm_cust_info )t
WHERE rn_or_flag_last != 1 



-- check nulls and duplicates in primary key
-- expectations : no result

SELECT prd_id,
	   COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- checking prd_key
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN 

-- Check for unwanted spaces
--expectations: no result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- Check for NULLs or Negative numbers
--expectations: no result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

--Check For invalid date orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- crm_sales_details
-->>getting column names
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = N'crm_sales_details'

SELECT name+','
FROM sys.columns
WHERE object_id = OBJECT_ID('silver.crm_sales_details')

--Check for invalid dates
SELECT 
	NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0  
OR LEN(sls_order_dt) > 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

--check for invalid date orders

SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check consistency between sales, quantity and price
-- >>> sales must be quantity * price
-->>> Values must not null, zero and negatives
SELECT sls_ord_num+',',
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	ABS(sls_price) AS sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 
			  OR sls_sales != sls_quantity * sls_price
		 THEN ABS(sls_price * sls_quantity) 
		 ELSE sls_sales
	END sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0

SELECT * FROM bronze.crm_sales_details


--- working on bronze.erp_cust_az12

SELECT * FROM bronze.erp_cust_az12

-->> checking unmatching data between cst_key and cid from bronze.crm_cust_info and erp_cust_az12 respectively
SELECT cid,
	   CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN (cid))
	        ELSE Cid
			END cid_new,
	   bdate,
	   gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN (cid))
	        ELSE Cid
			END NOT IN ( SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- working bdate of bronze.erp_cust_az12 
-->> identifying out of range dates
SELECT bdate
FROM bronze.erp_cust_az12 
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--working on gen of bronze.erp_cust_az12 
-->>data standardization and consistency

SELECT DISTINCT(gen)
FROM bronze.erp_cust_az12

SELECT DISTINCT(gen),
	   CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
	   END gen
FROM bronze.erp_cust_az12

--bronze.erp_loc_a101
--working on cid of bronze.erp_loc_a101 
-->> 
SELECT cid,
	   cntry
FROM bronze.erp_loc_a101 

SELECT cst_key FROM silver.crm_cust_info

-->> convert cid to match with cst_key and validate.
SELECT REPLACE(cid,'-',''),
	   cntry
FROM bronze.erp_loc_a101 
WHERE REPLACE(cid,'-','') NOT IN ( SELECT cst_key FROM silver.crm_cust_info)

--working on cntry of bronze.erp_loc_a101 
SELECT DISTINCT(cntry),
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
			 ELSE TRIM(cntry)
			 END cntry
FROM bronze.erp_loc_a101 

--bronze.erp_px_cat_g1v2
-->> working on id of bronze.erp_px_cat_g1v2

SELECT id,
	   cat,
	   subcat,
	   maintenance
FROM bronze.erp_px_cat_g1v2

-->> check unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id) OR cat != TRIM(cat) 
       OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-->> data standardization and consitency
SELECT DISTINCT maintenance -- checked all cat, subcat, maintenance one by one
FROM bronze.erp_px_cat_g1v2
 
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.silver_load AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME
		SET @start_batch_time = GETDATE()
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading into silver.crm_cust_info


		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info

		PRINT '>>Inserting Data into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM ( 
				SELECT 
				* ,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn_or_flag_last
				FROM bronze.crm_cust_info 
				WHERE cst_id IS NOT NULL)t
				WHERE rn_or_flag_last = 1

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'



		--Loading into silver.crm_prd_info

		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info

		PRINT '>>Inserting Data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt )

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --extract category id
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- extracted product id
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
				END prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
			FROM bronze.crm_prd_info

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'


		--Loading into silver.crm_sales_details
		---------------------------------
		--Working on the bronze.crm_sales_details  and insert in to the silver.crm_sales_details
		---------------------------------

		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details

		PRINT '>>Inserting Data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt= 0 OR LEN(sls_order_dt) !=8 THEN NULL 
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_ship_dt= 0 OR LEN(sls_ship_dt) !=8 THEN NULL 
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN sls_due_dt= 0 OR LEN(sls_due_dt) !=8 THEN NULL 
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 
					  OR sls_sales != ABS(sls_quantity * sls_price)
				 THEN ABS(sls_price * sls_quantity) --recalculated sales as sales if orignal values is missing or incorrect
				 ELSE sls_sales
			END sls_sales,
			sls_quantity,
			ABS(sls_price) AS sls_price -- dervived a price if the values are negatives
	
			FROM bronze.crm_sales_details

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		----Loading into silver.erp_cust_az12
		---------------------------------
		--Working on the bronze.erp_cust_az12 and insert in to the silver.erp_cust_az12
		---------------------------------

		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '>>Inserting Data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

		SELECT 
			   CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN (cid))
					ELSE cid
					END cid,-- matching cst_key of silver.crm_cust_info(already cleaned/transformed) to the cid of bronze.erp_cust_az12
	  
			   CASE WHEN bdate > GETDATE() THEN NULL
			   ELSE bdate
			   END bdate,--- Set the future birthdate to NULL

			   CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					ELSE 'n/a'
			   END gen ---Normalize gender values and handle unknown cases

			FROM bronze.erp_cust_az12

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'


		----Loading into silver.erp_loc_a101
		---------------------------------
		--Working on the bronze.erp_loc_a101 and insert in to the silver.erp_loc_a101
		---------------------------------

		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT '>>Inserting Data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid, cntry)

		SELECT REPLACE(cid,'-',''), -- standardize the cid to match with cst_key in silver.crm_cust_info
			   CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
					 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
					 ELSE TRIM(cntry)
					 END cntry  -- Normalize and handled missing or blank country codes
		FROM bronze.erp_loc_a101 

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'


		----Loading into silver.erp_px_cat_g1v2
		---------------------------------
		--Working on the bronze.erp_px_cat_g1v2 and insert in to the silver.erp_px_cat_g1v2
		---------------------------------
		-- Data quality of this table was good so did not performed any transeformation and loaded in to silver.erp_px_cat_g1v2

		SET @start_time = GETDATE()
		PRINT '>>Truncating table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		PRINT '>>Inserting Data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2( id,cat, subcat, maintenance)

		SELECT id,
			   cat,
			   subcat,
			   maintenance
		FROM bronze.erp_px_cat_g1v2


		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + 'Seconds'
		
		SET @end_batch_time = GETDATE()
		PRINT 'Loading silver layer is complete'
		PRINT 'Total loading time or duration: ' + CAST(DATEDIFF(SECOND,@start_batch_time,@end_batch_time) AS VARCHAR) + ' Seconds'
	END TRY

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END

EXEC silver.silver_load

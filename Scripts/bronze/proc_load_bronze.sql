CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE()
		PRINT '============================================='
		PRINT 'Loading Bronze Layer'
		PRINT '============================================='


		PRINT '---------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '---------------------------------------------'
	
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);


		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_crm\prd_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);


	
		PRINT '---------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '---------------------------------------------'
	
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting data into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting data into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\User\Desktop\subhan\SQL\Data Ware house projects\1 Data Ware house projects\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
	SET @batch_end_time= GETDATE()
	PRINT 'LOAD duration:' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds'
	END TRY
	BEGIN CATCH 
		PRINT '============================================='
		PRINT 'ERROR OCCURED DURING LOAD BRONZE LAYER'
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error message' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '============================================='
	END CATCH

END
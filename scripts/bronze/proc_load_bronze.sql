/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '============================================================';


		PRINT '------------------------------------------------------------';
		PRINT 'LOADING CRM FILE';
		PRINT '------------------------------------------------------------';

		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
	SET @start_time=GETDATE();		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> INSERTING DATA INTO TABLE: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (

				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>> Load Duration :'  + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINT '>> ----------------------------------'

		SET @start_time=GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.crm_prd_info')
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT('>> INSERTING DATA INTO TABLE: bronze.crm_prd_info')
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH  (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				)

		SET @end_time = GETDATE();

		PRINT '>> Load Duration :' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINT '>> ----------------------------------'  


		SET @start_time=GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.crm_sales_details')
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT('>> INSERTING DATA INTO TABLE: bronze.crm_sales_details')
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH  (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				)
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINT '------------------------------------------------------------';
		PRINT 'LOADING ERP FILE';
		PRINT '------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_cust_az12')
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_cust_az12')
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				)
		SET @end_time= GETDATE();
		PRINT '>> Loading  Time :' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds';


		SET	@start_time=GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_loc_a101')
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_loc_a101')
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				)
		SET @end_time=GETDATE();
		PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '>>-----------------------------';


		SET @start_time=GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2')
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_px_cat_g1v2')
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------';
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT('====================================================================')
		PRINT('ERROR HAPPENED DURING LOADING OF BRONZE LAYER')
		PRINT('ERROR NUMBER :'+ CAST(ERROR_NUMBER() AS NVARCHAR))
		PRINT('ERROR STATE :' + CAST(ERROR_STATE() AS NVARCHAR))
		PRINT('====================================================================')
	END CATCH

END;

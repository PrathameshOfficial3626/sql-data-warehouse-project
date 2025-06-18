---First Truncate and then load the table---
--- Creating Store Procedure -----
  
exec bronze.load_bronze
  
create or alter procedure bronze.load_bronze as
begin
	declare @bronze_start_time datetime,@bronze_end_time datetime;
	set @bronze_start_time= GETDATE()

			DECLARE @start_time datetime, @end_time datetime;
		BEGIN TRY
			print '======================================================================';
			print '=====loading Bronze Layer=======================================================';
			print '======================================================================';

			print '======================================================================';
			print '=====loading CRM Table=======================================================';
			print '======================================================================';


			SET @start_time = GETDATE();
			truncate table bronze.crm_cust_info
			bulk insert bronze.crm_cust_info
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
				firstrow= 2,
				fieldterminator = ',',
				Tablock
			);
			set @end_time = GETDATE();
			print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'



			set @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info
			bulk insert bronze.crm_prd_info
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
				FIRSTROW=2,
				FIELDTERMINATOR =',',
				TABLOCK
			);
			set @end_time= getdate();
			print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'



			set @start_time = getdate()
			truncate table bronze.crm_sales_details
			BULK INSERT bronze.crm_sales_details
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time= getdate()
			print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'


			print '======================================================================';
			print '=====loading ERP Table=======================================================';
			print '======================================================================';

			set @start_time= getdate()
			truncate table bronze.erp_cust_az12
			BULK INSERT bronze.erp_cust_az12
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE()
			print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'


			set @start_time= GETDATE()
			truncate table bronze.erp_loc_a101
			BULK INSERT bronze.erp_loc_a101
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time= getdate()
			print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'


			set @start_time= getdate()
			truncate table bronze.erp_px_cat_g1v2
			BULK INSERT bronze.erp_px_cat_g1v2
			from 'C:\Users\TejalPrathmesh\OneDrive\Desktop\sql_Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
		)
		set @end_time = getdate()
		print '>> Load Duration' + cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds'
			print '----------------------------------------------'
		END TRY

		BEGIN CATCH
			PRINT '================================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR_NO ' + CAST(ERROR_NUMBER() AS NVARCHAR(50))
			PRINT '================================================='

		END CATCH
		set @bronze_end_time = getdate()
end


print 'Total time taken to load bronze layer = '+ cast(datediff(second, @bronze_start_time,@bronze_end_time)as varchar) + ' SECONDS'

        

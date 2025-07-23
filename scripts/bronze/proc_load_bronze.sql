/**************************************************************************************************
* Purpose:
* Defines `bronze.load_bronze`, a repeatable ETL‑staging procedure that refreshes all Bronze‑layer
* tables with the latest raw data from CSV extracts.  The procedure
*   1. Logs batch start/end timestamps and per‑table load durations.
*   2. Truncates each Bronze table (CRM and ERP) to guarantee idempotent reloads.
*   3. Performs `BULK INSERT` from local CSV files into:
*        • bronze.crm_cust_info      • bronze.crm_prd_info
*        • bronze.crm_sales_details  • bronze.erp_cust_az12
*        • bronze.erp_loc_a101       • bronze.erp_px_cat_g1v2
*   4. Emits console banners for progress visibility.
*   5. Catches and prints any runtime errors.

* Why run it?
* ► To keep the Bronze tier in a known‑clean state before downstream Silver/Gold transforms.
* ► To benchmark load performance via the printed timings.

* Warning:
* ⚠️ Each execution **truncates** all Bronze tables—any existing data is irreversibly deleted.
* ⚠️ CSV file paths are hard‑coded; adjust for your environment or externalize to parameters.
* ⚠️ For development/test use.  Validate file sizes, formats, and locks before executing in prod.

* Author: Oluwafemi Popoola
* Date  : 23rd July, 2025
**************************************************************************************************/

-- Create or update a stored procedure named 'load_bronze' in the 'bronze' schema
create or alter procedure bronze.load_bronze as
begin
declare @start_time as datetime, declare @end_time as datetime, declare @batch_start_time as datetime, declare @batch_end_time as datetime;
begin try
	set @batch_start_time = getdate();
		PRINT ‘============================================================’;
		PRINT ‘LOADING THE BRONZE LAYER’;
		PRINT ‘============================================================’;


		PRINT ‘------------------------------------------------------------‘;
		PRINT ‘LOADING THE CRM TABLES’;
		PRINT ‘------------------------------------------------------------‘;

-- Clear all existing data from the 'crm_cust_info' table before loading fresh data
set @start_time = getdate();
    		truncate table bronze.crm_cust_info;

    		-- Load data from a CSV file into 'crm_cust_info' using BULK INSERT
    		bulk insert bronze.crm_cust_info
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.crm_cust_info.csv'
    		with(
        		firstrow = 2,               -- Skip the header row
        		fieldterminator = ',',      -- Columns are separated by commas
        		tablock                      -- Lock the table for performance
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’


    		-- Repeat the same steps for 'crm_prd_info'
set @start_time = getdate();
truncate table bronze.crm_prd_info;
    		bulk insert bronze.crm_prd_info
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.crm_prd_info.csv'
    		with(
        		firstrow = 2,
        		fieldterminator = ',',
        		tablock
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’


    		-- Repeat for 'crm_sales_details'
set @start_time = getdate();
truncate table bronze.crm_sales_details;
    		bulk insert bronze.crm_sales_details
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.crm_sales_details.csv'
    		with(
        		firstrow = 2,
        		fieldterminator = ',',
        		tablock
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’


		PRINT ‘------------------------------------------------------------‘;
		PRINT ‘LOADING THE ERP TABLES’;
		PRINT ‘------------------------------------------------------------‘;

    		-- Repeat for 'erp_cust_az12'
set @start_time = getdate();
truncate table bronze.erp_cust_az12;
    		bulk insert bronze.erp_cust_az12
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.erp_cust_az12.csv'
    		with(
        		firstrow = 2,
        		fieldterminator = ',',
        		tablock
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’


    		-- Repeat for 'erp_loc_a101'
set @start_time = getdate();
truncate table bronze.erp_loc_a101;
    		bulk insert bronze.erp_loc_a101
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.erp_loc_a101.csv'
    		with(
        		firstrow = 2,
        		fieldterminator = ',',
        		tablock
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’


    		-- Repeat for 'erp_px_cat_g1v2'
		set @start_time = getdate();
    		truncate table bronze.erp_px_cat_g1v2;
    		bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\BIZMARROW\Desktop\Folder\Maths for ML\SQL Practical\sql-data-analytics-project\datasets\csv-files\bronze.erp_px_cat_g1v2.csv'
    		with(
        		firstrow = 2,
        		fieldterminator = ',',
        		tablock
    		);
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’;
PRINT ‘>>………………………………………’;

set @batch_end_time = getdate();
PRINT ‘=================================================================’;
PRINT ‘BRONZE LAYER LOADING COMPLETED’;
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @batch_start_time, @batch_end_time) as nvarchar) + ‘ seconds’;
PRINT ‘=================================================================’;

	end try
	begin catch
		PRINT ‘============================================================’;
		PRINT ‘ERROR OCCURRED DURING LAODING OF BRONZE LAYER’;
		PRINT ‘Error Message: ‘+ ERROR_MESSAGE ();
		PRINT ‘Error Message: ‘+ CAST (ERROR_NUMBER () AS NVARCHAR);
		PRINT ‘Error Message: ‘+ CAST (ERROR_STATE () AS NVARCHAR);
		PRINT ‘============================================================’;
	end catch
end

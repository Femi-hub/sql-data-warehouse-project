/*
Purpose:
--------
This stored procedure `silver.load_silver` is responsible for transforming and loading cleaned data 
from the Bronze layer into the Silver layer of a data warehouse, following the medallion architecture pattern.

Key functionalities:
---------------------
1. Cleans, transforms, and inserts data into six Silver tables:
   - silver.crm_cust_info        : Customer information (de-duplicated and standardized)
   - silver.crm_prd_info         : Product data (with derived category and end date)
   - silver.crm_sales_details    : Sales transactions (validated, recalculated fields)
   - silver.erp_cust_az12        : ERP customer records (cleaned gender, date, ID)
   - silver.erp_loc_a101         : Customer location info (standardized country values)
   - silver.erp_px_cat_g1v2      : Product category and maintenance data (direct load)

2. Implements various data quality improvements:
   - Trims and standardizes string values (e.g., gender, marital status)
   - Handles missing or invalid data (e.g., future birthdates, null sales)
   - Calculates derived fields (e.g., product end dates, recalculated prices/sales)

3. Uses TRY...CATCH for error handling and captures batch load durations for each section 
   to support monitoring and performance tracking.

This script supports reliable data transformation between raw and analytics-ready layers, 
helping ensure data consistency and quality across reporting and analysis platforms.
*/

-- Create or alter a stored procedure to load cleaned data into the 'silver' schema
create or alter procedure silver.load_silver as
begin 
  declare @start_time as datetime, declare @end_time as datetime, declare @batch_start_time as datetime, declare @batch_end_time as datetime;
  begin try
  set @batch_start_time = getdate();
    
-- Step 1: Clean and load customer info (crm_cust_info)
  set @start_time = getdate();

  truncate table silver.crm_cust_info;
  insert into silver.crm_cust_info (
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
  )
  select 
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
  from (
      select 
          cst_id,
          cst_key,
          trim(cst_firstname) as cst_firstname,
          trim(cst_lastname) as cst_lastname,
      -- Normalize marital status codes
          case
              when upper(trim(cst_marital_status)) = 'S' then 'Single'
              when upper(trim(cst_marital_status)) = 'M' then 'Married'
              else 'n/a'
          end as cst_marital_status,
      -- Normalize gender codes
          case
              when upper(trim(cst_gndr)) = 'F' then 'Female'
              when upper(trim(cst_gndr)) = 'M' then 'Male'
              else 'n/a'
          end as cst_gndr,
          cst_create_date,
      -- Remove duplicates: Keep the latest record per customer
        ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as list
      from bronze.crm_cust_info
      where cst_id is not null
  ) t
  where list = 1;  -- Only insert the most recent row per customer
  set @end_time = getdate();
  PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
  PRINT ‘>>………………………………………’

        -- Step 2: Clean and load product info (crm_prd_info)
  set @start_time = getdate();

  truncate table silver.crm_prd_info;
  insert into silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    select 
        prd_id,
        replace(substring(prd_key, 1, 5), '-', '_') as cat_id,  -- Extract category from product key
        substring(prd_key, 7, LEN(prd_key)) as prd_key,     -- Extract product key portion
        prd_nm,
        isnull(prd_cost, 0) as prd_cost,      -- Default missing cost to 0
        	-- Normalize product line values
        case upper(trim(prd_line))
            	when 'R' then 'Road'
            	when 'S' then 'Other Sales'
           		when 'M' then 'Mobile'
            	when 'T' then 'Touring'
            	else 'n/a'
        end as prd_line,
        prd_start_dt,
        	-- Calculate end date as 1 day before next start date (if exists)
        dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
    from bronze.crm_prd_info;
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’

    	-- Step 3: Clean and load sales details (crm_sales_details)
set @start_time = getdate();
truncate table silver.crm_sales_details;

insert into silver.crm_sales_details (
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
select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
        	-- Validate and convert order date
    case 
        when len(sls_order_dt) != 8 or sls_order_dt <= 0 then null
        else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
  -- Convert shipping and due dates
    cast(cast(sls_ship_dt as varchar) as date) as sls_ship_dt,
    cast(cast(sls_due_dt as varchar) as date) as sls_due_dt,
  -- Recalculate sales if invalid or mismatched
    case
        when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
        else sls_sales 
    end as sls_sales,
    sls_quantity,
        	-- Recalculate price if invalid or missing
    case 
        when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity, 0) 
        else sls_price 
    end as sls_price
from bronze.crm_sales_details;
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’

    	-- Step 4: Clean and load ERP customer data (erp_cust_az12)
set @start_time = getdate();

truncate table silver.erp_cust_az12;
insert into silver.erp_cust_az12 (
      cid,
      bdate,
      gen
)
select 
  -- Clean up customer ID prefix
    case 
        when cid like 'NAS%' then substring(cid, 4, LEN(cid))
        else cid
    end as cid,
  -- Remove birthdates that are in the future
    case
        when bdate > GETDATE() then null
        else bdate
    end as bdate,
  -- Normalize gender values
    case 
        when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        when upper(trim(gen)) in ('M', 'MALE') then 'Male'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12;
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’

    	-- Step 5: Clean and load ERP location data (erp_loc_a101)
set @start_time = getdate();

truncate table silver.erp_loc_a101;
insert into silver.erp_loc_a101 (
    cid,
    cntry
)
select 
    replace(cid, '-', '') as cid,  -- Remove dashes from customer ID
  -- Normalize country names
    case
        when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US', 'USA') then 'United States'
        when trim(cntry) in (null, '') then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101;
set @end_time = getdate();
PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’
PRINT ‘>>………………………………………’

    	-- Step 6: Direct load of ERP category data (erp_px_cat_g1v2)
set @start_time = getdate();

truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
select 
    id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;
    set @end_time = getdate();
    PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @start_time, @end_time) as nvarchar) + ‘ seconds’;
    PRINT ‘>>………………………………………’;

    set @batch_end_time = getdate();
    PRINT ‘=================================================================’;
    PRINT ‘SILVER LAYER LOADING COMPLETED’;
    PRINT ‘>> LOAD DURATION: ‘+ cast (datediff (second, @batch_start_time, @batch_end_time) as nvarchar) + ‘ seconds’;
    PRINT ‘=================================================================’;
end try
begin catch
		PRINT ‘============================================================’;
		PRINT ‘ERROR OCCURRED DURING LOADING OF SILVER LAYER’;
		PRINT ‘Error Message: ‘+ ERROR_MESSAGE ();
		PRINT ‘Error Message: ‘+ CAST (ERROR_NUMBER () AS NVARCHAR);
		PRINT ‘Error Message: ‘+ CAST (ERROR_STATE () AS NVARCHAR);
		PRINT ‘============================================================’;
	end catch
end


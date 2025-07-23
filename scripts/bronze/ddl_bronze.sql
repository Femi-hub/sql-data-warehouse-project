/**************************************************************************************************
* Purpose:
* This script creates foundational raw data tables under the 'bronze' schema in the 'newdata' 
* database. These tables are designed to hold minimally processed data sourced from CRM and ERP 
* systems. It follows a pattern of checking for existing tables and dropping them before recreating 
* fresh ones to ensure a clean staging environment for ETL processes.

* Tables created:
*   - bronze.crm_cust_info       : Customer information from the CRM system
*   - bronze.crm_prd_info        : Product information from the CRM system
*   - bronze.crm_sales_details   : Sales transaction details from the CRM system
*   - bronze.erp_cust_az12       : Customer demographic data from the ERP system
*   - bronze.erp_loc_a101        : Customer location data from the ERP system
*   - bronze.erp_px_cat_g1v2     : Product category data from the ERP system

* Note:
* ⚠️ This script will drop any existing tables with the same names before creating new ones.
* ⚠️ All existing data in these tables will be lost upon execution.
* ⚠️ This script is intended for development or ETL staging environments, not for production use.

* Author: Oluwafemi Popoola
* Date: 23rd July, 2025
**************************************************************************************************/

if OBJECT_ID('bronze.crm_cust_info') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);
go

if OBJECT_ID('bronze.crm_prd_info') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date
);
go

if OBJECT_ID('bronze.crm_sales_details') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details (
	prd_end_dt nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
go

if OBJECT_ID('bronze.erp_cust_az12') is not null
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);
go

if OBJECT_ID('bronze.erp_loc_a101') is not null
	drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	cid nvarchar(50),
	cntry nvarchar(50)
);
go

if OBJECT_ID('bronze.erp_px_cat_g1v2') is not null
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);
go

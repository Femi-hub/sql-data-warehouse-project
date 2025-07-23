/*
Purpose:
--------
This SQL script is designed to create cleaned and structured tables in the 'silver' schema 
as part of a data warehousing and ETL pipeline. It performs the following:

1. Drops existing versions of silver-layer tables (if they exist) to ensure a clean slate.
2. Recreates the tables with clearly defined schema structures for:
   - Customer information (CRM and ERP sources)
   - Product information
   - Sales transaction details
   - Location data
   - Product category and maintenance details

These tables serve as the "silver layer" in a medallion architecture, where raw data 
is transformed into cleaned, validated, and business-ready datasets for downstream analytics.
*/

-- If the 'crm_cust_info' table already exists in the 'silver' schema, drop it
if OBJECT_ID('silver.crm_cust_info') is not null
    drop table silver.crm_cust_info;

-- Create the 'crm_cust_info' table in the 'silver' schema
-- This table stores cleaned customer information
create table silver.crm_cust_info (
    cst_id int,                      -- Customer ID
    cst_key nvarchar(50),           -- Unique customer key
    cst_firstname nvarchar(50),     -- Customer's first name
    cst_lastname nvarchar(50),      -- Customer's last name
    cst_marital_status nvarchar(50),-- Marital status
    cst_gndr nvarchar(50),          -- Gender
    cst_create_date date            -- Date the customer record was created
);
go

-- If the 'crm_prd_info' table already exists, drop it
if OBJECT_ID('silver.crm_prd_info') is not null
    drop table silver.crm_prd_info;

-- Create the 'crm_prd_info' table to store cleaned product information
create table silver.crm_prd_info (
    prd_id int,                     -- Product ID
    cat_id nvarchar(50),           -- Category ID
    prd_key nvarchar(50),          -- Product key
    prd_nm nvarchar(50),           -- Product name
    prd_cost int,                  -- Cost of the product
    prd_line nvarchar(50),         -- Product line
    prd_start_dt date,             -- Product start date
    prd_end_dt date                -- Product end date
);
go

-- If the 'crm_sales_details' table exists, drop it
if OBJECT_ID('silver.crm_sales_details') is not null
    drop table silver.crm_sales_details;

-- Create the 'crm_sales_details' table for cleaned sales transaction records
create table silver.crm_sales_details (
    sls_ord_num nvarchar(50),      -- Sales order number
    sls_prd_key nvarchar(50),      -- Product key sold
    sls_cust_id int,               -- Customer ID
    sls_order_dt date,             -- Order date
    sls_ship_dt date,              -- Shipping date
    sls_due_dt date,               -- Due date
    sls_sales int,                 -- Sales amount
    sls_quantity int,              -- Quantity sold
    sls_price int                  -- Price per unit
);
go

-- If the 'erp_cust_az12' table exists, drop it
if OBJECT_ID('silver.erp_cust_az12') is not null
    drop table silver.erp_cust_az12;

-- Create the 'erp_cust_az12' table for cleaned ERP customer data
create table silver.erp_cust_az12 (
    cid nvarchar(50),              -- Customer ID
    bdate date,                    -- Birthdate
    gen nvarchar(50)               -- Gender
);
go

-- If the 'erp_loc_a101' table exists, drop it
if OBJECT_ID('silver.erp_loc_a101') is not null
    drop table silver.erp_loc_a101;

-- Create the 'erp_loc_a101' table for cleaned location data
create table silver.erp_loc_a101 (
    cid nvarchar(50),              -- Customer ID
    cntry nvarchar(50)             -- Country
);
go

-- If the 'erp_px_cat_g1v2' table exists, drop it
if OBJECT_ID('silver.erp_px_cat_g1v2') is not null
    drop table silver.erp_px_cat_g1v2;

-- Create the 'erp_px_cat_g1v2' table for cleaned product category and maintenance info
create table silver.erp_px_cat_g1v2 (
    id nvarchar(50),               -- Identifier
    cat nvarchar(50),              -- Category
    subcat nvarchar(50),           -- Sub-category
    maintenance nvarchar(50)       -- Maintenance details
);
go

--------------------
--Quality Checks
--------------------
/*
Purpose:
--------
This script performs **data quality checks** on the cleaned Silver layer tables within a medallion data architecture. 
It ensures that the transformed data is reliable, standardized, and ready for downstream analytics and reporting.

Key quality checks performed include:

1. **Primary Key Integrity**:
   - Detects nulls or duplicate values in primary key columns (e.g., `cst_id`, `prd_id`) which could impact joins or aggregations.

2. **Whitespace Validation**:
   - Checks for unwanted leading/trailing spaces in critical string fields like customer names, product names, and categorical attributes.

3. **Data Standardization & Consistency**:
   - Reviews distinct values of categorical fields (e.g., `cst_gndr`, `prd_line`, `gen`, `cat`) to verify normalization across the dataset.

4. **Value Validations**:
   - Flags invalid or missing numerical values (e.g., negative or null product cost, sales amounts, quantities, prices).

5. **Date Validations**:
   - Ensures logical date sequences (e.g., product start vs end dates, order vs ship/due dates).
   - Flags out-of-range or future dates (e.g., birthdates beyond today's date).

6. **Business Rule Enforcement**:
   - Verifies that `sales = quantity * price` for each transaction record.
   - Ensures all financial and transactional metrics are non-null and positive.

This script is designed to be run after loading data into the Silver layer to detect anomalies early 
and maintain high-quality, trustworthy datasets for analytics and business intelligence.
*/

-----------------------------------------------
--Checking silver.crm_cust_info
-----------------------------------------------
--Check for nulls or duplicates in Primary Key
--Expectation: No Result
SELECT cst_id
	,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for unwanted spaces
--Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-----------------------------------------------
--Checking silver.crm_prd_info
-----------------------------------------------
--Check for nulls or duplicates in Primary Key
--Expectation: No Result
SELECT prd_id
	,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check for nulls or negative numbers
--Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--Check for invalid date orders
--Expectation: No Results
SELECT *
FROM silver.crm_prd_info
Where prd_end_dt < prd_start_dt

-----------------------------------------------
--Checking silver.crm_sales_details
-----------------------------------------------
--Check for invalid date orders
--Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check data consistency between Sales, Quantity & Price
--Sales = Quantity * Price
--Values must not be null
--Expectation: No Results

SELECT DISTINCT sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-----------------------------------------------
--Checking silver.erp_cust_az12
-----------------------------------------------
--Identify out-of-range dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < ‘1924-01-01 OR bdate > getdate()

--Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-----------------------------------------------
--Checking silver.erp_loc_a101
-----------------------------------------------
--Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

-----------------------------------------------
--Checking silver.erp_px_cat_g1v2
-----------------------------------------------
--Check for unwanted spaces
--Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--Data Standardization & Consistency
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2


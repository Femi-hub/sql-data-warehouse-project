-- =============================================
-- PURPOSE OF QUALITY CHECKS
-- =============================================

-- These queries are used to validate the integrity and consistency of 
-- the data in the Silver and Gold layers before loading into BI dashboards 
-- or analytics tools. The checks focus on duplicate detection, data standardization, 
-- null/missing values, and referential integrity. Below are the specific purposes:

-- 1. Duplicate Customer Check:
--    Identifies duplicate records in `silver.crm_cust_info` based on `cst_id`.
--    Ensures each customer ID is unique after enrichment with ERP data.

-- 2. Gender Consistency Check:
--    Compares gender fields from CRM and ERP sources to identify inconsistencies 
--    or potential mapping issues before applying fallback logic.

-- 3. Gender Mapping Verification:
--    Validates the logic used in `gold.dim_customers` to derive a unified gender field, 
--    confirming correct application of fallback from ERP when CRM value is 'n/a'.

-- 4. Duplicate Product Key Check:
--    Ensures that each `prd_key` is unique among active products 
--    in `silver.crm_prd_info`, avoiding duplication in the product dimension.

-- 5. Referential Integrity in Gold Layer:
--    Detects unmatched foreign keys in `gold.fact_sales`, identifying sales records 
--    that lack corresponding entries in `gold.dim_customers` or `gold.dim_products`. 
--    Helps catch issues in join logic or missing dimension data.

-- These checks are essential for maintaining trust in reporting outputs 
-- and ensuring data warehouse reliability.
-- =============================================

-- =============================================
-- 1. Duplicate Customer Check
-- Purpose: Ensures uniqueness of customer records in `silver.crm_cust_info`
--          by detecting duplicate `cst_id` values after enrichment joins.
-- Expectation: No results (i.e., each `cst_id` should appear only once)
-- =============================================
select cst_id
    ,COUNT(*)
from (
    select c.cst_id
        ,c.cst_key
        ,c.cst_firstname
        ,c.cst_lastname
        ,c.cst_marital_status
        ,c.cst_gndr
        ,c.cst_create_date
        ,e.bdate
        ,e.gen
        ,l.cntry
    from silver.crm_cust_info c
    left join silver.erp_cust_az12 e
        on e.cid = c.cst_key
    left join silver.erp_loc_a101 l
        on l.cid = c.cst_key
) t
group by cst_id
having COUNT(*) > 1;

-- =============================================
-- 2. Gender Consistency Check
-- Purpose: Compares gender values from CRM (`cst_gndr`) and ERP (`gen`)
--          to identify mismatches or discrepancies before standardization.
-- Expectation: Review unique combinations to assess consistency.
-- =============================================
select distinct c.cst_gndr
    ,e.gen
from silver.crm_cust_info c
left join silver.erp_cust_az12 e
    on e.cid = c.cst_key
left join silver.erp_loc_a101 l
    on l.cid = c.cst_key
order by 1, 2;

-- =============================================
-- 3. Gender Mapping Verification
-- Purpose: Validates the fallback logic for gender mapping used in `gold.dim_customers`,
--          where ERP gender is used when CRM gender is 'n/a'.
-- Expectation: Ensure `new_cst_gndr` is populated accurately.
-- =============================================
select distinct c.cst_gndr
    ,e.gen
    ,case 
        when c.cst_gndr != 'n/a' then c.cst_gndr
        else coalesce(e.gen, 'n/a')
    end as new_cst_gndr
from silver.crm_cust_info c
left join silver.erp_cust_az12 e
    on e.cid = c.cst_key
left join silver.erp_loc_a101 l
    on l.cid = c.cst_key
order by 1, 2;

-- =============================================
-- 4. Duplicate Product Key Check
-- Purpose: Detects duplicate product keys (`prd_key`) in the active product list
--          to prevent duplication in `gold.dim_products`.
-- Expectation: No results (each `prd_key` should be unique).
-- =============================================
select prd_key
    ,COUNT(*)
from (
    select p.prd_id
        ,p.cat_id
        ,p.prd_key
        ,p.prd_nm
        ,p.prd_cost
        ,p.prd_line
        ,p.prd_start_dt
        ,c.cat
        ,c.subcat
        ,c.maintenance
    from silver.crm_prd_info p
    left join silver.erp_px_cat_g1v2 c
        on c.id = p.cat_id
    where prd_end_dt is null
) t
group by prd_key
having COUNT(*) > 1;

-- =============================================
-- 5. Referential Integrity Check in Gold Layer
-- Purpose: Identifies orphaned records in `gold.fact_sales` that do not have
--          matching dimension records in `gold.dim_customers` or `gold.dim_products`.
-- Expectation: No null dimension keys in joins (i.e., no broken links).
-- =============================================
select *
from gold.fact_sales s
left join gold.dim_customers c
    on c.customer_key = s.customer_key
left join gold.dim_products p
    on p.product_key = s.product_key
where p.product_key is null;

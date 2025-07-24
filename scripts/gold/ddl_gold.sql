-- =============================================
-- PURPOSE OF VIEWS IN THIS SCRIPT
-- =============================================

-- 1. gold.fact_sales:
--    Creates a transactional fact table by joining cleaned sales records 
--    from the Silver Layer with corresponding product and customer dimension keys.
--    Enables analytical reporting on sales performance, revenue, quantity sold, 
--    and time-based trends.

-- 2. gold.dim_products:
--    Builds the product dimension by combining product metadata with category 
--    information from ERP sources. Filters for active products only and supports 
--    slicing sales data by product attributes like category, subcategory, cost, 
--    and product line.

-- 3. gold.dim_customers:
--    Creates the customer dimension by enriching CRM data with ERP details 
--    including birthdate, gender, and country. Standardizes values and resolves 
--    missing fields to enable robust customer profiling, segmentation, and 
--    demographic analysis.

-- These views form the Gold Layer of the data warehouse and adhere to 
-- a star schema model for efficient and flexible BI reporting.

create view gold.fact_sales as
select sls_ord_num as order_number
	,product_key 
	,customer_key
	,sls_order_dt as order_date
	,sls_ship_dt as shipping_date
	,sls_due_dt as due_date
	,sls_sales as sales_amount
	,sls_quantity as quantity
	,sls_price as price
from silver.crm_sales_details s
left join gold.dim_customers c
on c.customer_id = s.sls_cust_id
left join gold.dim_products p
on p.product_number = s.sls_prd_key

create view gold.dim_products as
select ROW_NUMBER() over (order by prd_key) as product_key
	,p.prd_id as product_id
	,p.prd_key as product_number
	,p.prd_nm as product_name
	,p.cat_id as category_id
	,c.cat as category
	,c.subcat as subcategory
	,c.maintenance
	,p.prd_cost as cost
	,p.prd_line as product_line
	,p.prd_start_dt as start_date
from silver.crm_prd_info p
left join silver.erp_px_cat_g1v2 c
on c.id = p.cat_id
where prd_end_dt is null;

create view gold.dim_customers as
select ROW_NUMBER() over (order by cst_id)  as customer_key
		,c.cst_id as customer_id
		,c.cst_key as customer_number
		,c.cst_firstname as firstname
		,c.cst_lastname as lastname
		,l.cntry as country
		,c.cst_marital_status as marital_status
		,case 
			when c.cst_gndr != 'n/a' then c.cst_gndr
			else coalesce(e.gen, 'n/a')
		end as gender
		,e.bdate as birthdate
		,c.cst_create_date as create_date
from silver.crm_cust_info c
left join silver.erp_cust_az12 e
on e.cid = c.cst_key
left join silver.erp_loc_a101 l
on l.cid = c.cst_key;


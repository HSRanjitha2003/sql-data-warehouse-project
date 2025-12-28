/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
select
	ROW_NUMBER() over(order by cst_id) customer_key,   -- Surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key As customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry as country,
	ci.cst_material_status AS martal_status,
	case when ci.cst_gndr!='n/a' then ci.cst_gndr -- CRM is the master table for gender
		 else coalesce(ca.gen,'n/a')
	end as gender,
	ca.bdate as birthdate,
	ci.cst_create_date AS create_date
	
from 
	silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON			ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON          ci.cst_key=la.cid ;

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO


CREATE VIEW gold.dim_products AS
select  ROW_NUMBER() over(order by p1.prd_start_dt,p1.prd_key) as product_key,
		p1.prd_id AS product_id ,
        p1.prd_key AS product_number,
		p1.prd_nm AS product_name,
		p1.cat_id AS category_id,
		p2.cat AS category,
		p2.subcat AS subcategory,
		p2.maintenance,
		p1.prd_cost as cost,
		p1.prd_line as product_line,
		p1.prd_start_dt	as start_date	 
from silver.crm_prd_info p1 
left join silver.erp_px_cat_g1v2 p2 
on p1.cat_id=p2.id where prd_end_dt is null;

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales as

select sd.sls_ord_num as order_number,
      pi.product_key,
      ci.customer_key,
      sd.sls_order_dt as order_date,
      sd.sls_ship_dt as shipping_date,
      sd.sls_due_dt as due_date,
      sd.sls_sales as sales_amount,
      sd.sls_quantity as quantity,
      sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_customers ci
on sd.sls_cust_id=ci.customer_id
left join gold.dim_products pi
on sd.sls_prd_key=pi.product_number;

go

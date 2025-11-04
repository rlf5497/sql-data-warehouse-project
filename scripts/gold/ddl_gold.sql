/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
	This script defines the Gold layer in the data warehouse using views
	that represents business-ready, analytics-focused tables. Typically:
		- Dimension tables (e.g., customers, products)
		- Fact tables (e.g., sales transactions).

	This Gold Layer is built upon the Silver Layer and serves as the final stage
	in the Medallion Architecture, optimized for star schema modeling and
	directly consumable by BI tools, dashboards, or analytical queries.

Key Characteristics:
	- Star Schema layout: Fact tables linked via surrogate keys to dimensions.
	- Fields are clean, enriched, and aligned to business terminilogy.
	- No raw or intermediate fields are exposed.

Usage:
	- These views are queried by analyst, data scientists, or BI engineers.
	- Use Gold Layer objects for reporting, KPIs, and dashboard metrics.
===============================================================================
*/


-- ====================================================================
-- Dimension: gold.dim_customers
-- Represents customer attributes joined from multiple Silver sources
-- Purpose: Provides enriched customer details and lookup for customer-based analysis
-- ====================================================================

CREATE OR REPLACE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC) 	AS customer_key, 		-- Surrogate primary key
	ci.cst_id 										AS customer_id, 		-- Natural ID from CRM
	ci.cst_key 										AS customer_number, 	-- Business-defined key
	ci.cst_firstname 								AS first_name,
	ci.cst_lastname 								AS last_name,
	la.cntry  										AS country,				-- From location master
	ci.cst_marital_status 							AS marital_status,
	-- Gender preference: prioritized from customer info unless 'n/a'
	CASE
		WHEN	ci.cst_gndr != 'n/a'	THEN ci.cst_gndr
		ELSE	COALESCE(ca.gen, 'n/a')
	END 											AS gender,
	ca.bdate 										AS birthdate,			-- From ERP
	ci.cst_create_date 								AS create_date			
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	   ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
	   ON ci.cst_key = la.cid;



-- ====================================================================
-- Dimension: gold.dim_products
-- Represents enriched product details and hierarchy information
-- Purpose: Product-level analysis by category/subcategory
-- ====================================================================

CREATE OR REPLACE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) 	AS product_key,		-- Surrogate Key
	pi.prd_id 													AS product_id,
	pi.prd_key 													AS product_number,
	pi.prd_nm 													AS product_name,
	pi.cat_id 													AS category_id,
	px.cat 														AS category,		-- From product category table	
	px.subcat 													AS subcategory,
	px.maintenance,																	-- Maintenance category
	pi.prd_cost 												AS cost,
	pi.prd_line 												AS product_line,
	pi.prd_start_dt 											AS start_date		-- Product active date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS px
	   ON pi.cat_id = px.id
WHERE
	pi.prd_end_dt IS NULL;															-- Only active products



-- ====================================================================
-- Fact: gold.fact_sales
-- Represents transactional sales metrics joined with dimension tables
-- Purpose: Central fact table for calculating revenue, quantity, price, etc.
-- ====================================================================

CREATE OR REPLACE VIEW gold.fact_sales AS 
SELECT
	sd.sls_ord_num 							AS order_number,	-- Transaction identifier
	pr.product_key,												-- FK reference to product dimension
	cu.customer_key,											-- FK reference to customer dimension
	sd.sls_order_dt 						AS order_date,		-- Order placement date
	sd.sls_ship_dt 							AS shipping_date,	-- Shipment date
	sd.sls_due_dt 							AS due_date,		-- Expected delivery date
	sd.sls_sales 							AS sales_amount,	-- Total revenue of the order line
	sd.sls_quantity 						AS quantity,		-- Quantity sold
	sd.sls_price 							AS price			-- Unit Price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS cu
	   ON sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products AS pr
	   ON sd.sls_prd_key = pr.product_number;



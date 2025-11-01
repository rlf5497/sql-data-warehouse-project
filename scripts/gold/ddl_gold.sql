/*
DDL Script: Create Gold Views
Script Purpose
	This script create views for the Gold Layer in the data warehouse
	The Gold Layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver Layer
	to product a clean, enriched, and business-ready dataset.

Usage:
	- These views can be quried directly for analytics and reporting
*/


-- Create Dimension: gold.dim_customers
CREATE OR REPLACE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry  AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN	ci.cst_gndr != 'n/a'	THEN ci.cst_gndr
		ELSE	COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	   ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
	   ON ci.cst_key = la.cid;

	

-- Data Integration
-- Give Friendly Names, meaningful names
-- Sort the Columns into logical groups to improve readability.


-- Dimension Customer Table
CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id ASC) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
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

-- Surrogate Key
-- System gemerated unique identifier assigned to each record in a table, to make the
-- the record unique. It is not a business key, it has no meaning and no one in the business
-- knows about it, we only use it in order to connect our data model.

-- A new primary key in the data warehouse

-- DDL-based generation
-- Query-based using Window Function (Row_Number)
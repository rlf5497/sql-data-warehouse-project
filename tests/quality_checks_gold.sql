/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Script Purpose:
	This script performs data quality checks on the Gold Layer of the Data Warehouse.
	The Gold Layer represents the final, business-ready data (fact and dimension views)
	user for analytics and reporting.

	These quality checks validate:
		- Uniqueness of surrogate keys in dimension tables.
		- Referential integrity between fact and dimension tables.
		- Proper relationship and consistency in the star schema model.

Usage Notes:
	- Run this script after creating and populating Gold Layer views.
	- Any rows returned by these queries may indicate data quality issues
	  that require investigation and correction in the upstream layers.
	  (Silver or Bronze)
===============================================================================
*/



-- ===============================================================
-- Checking gold.dim_customers
-- ===============================================================
-- Purpose:
--		Validate that each customer_key in gold.dim_customers is unique.
-- Expectation:
--		No duplicate rows should exist for customer_key
-- ===============================================================

SELECT
	customer_key,
	COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY
	customer_key
HAVING
	COUNT(*) > 1;



-- ===============================================================
-- Checking gold.dim_products
-- ===============================================================
-- Purpose:
-- 		Ensure uniqueness of product_key in gold.dim_products
-- Expectation:
--		Each product_key should appear only once.
-- ===============================================================

SELECT
	product_key,
	COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY
	product_key
HAVING
	COUNT(*) > 1;



-- ===============================================================
-- Checking gold.fact_sales
-- ===============================================================
-- Purpose:
--		Validate referential integrity between the fact and dimension tables.
--		This ensures that every record in fact_sales correctly references
--		existing keys in dim_customers and dim_products
-- Expectation:
--		No NULL values should appear in the joined dimension keys.
-- ===============================================================

SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
	   ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
	   ON f.product_key = p.product_key
WHERE
	c.customer_key IS NULL
OR	p.product_key IS NULL;

/*
Cleaned bronze.crm_cust_info table.
	1. Check for NULLS or Duplicates in Primary Key (cst_id).
		- Pick the latest record (cst_create_date)
	2. Check for Unwanted Spaces in String Values
	3. Data Standardization & Consistency
		- Store clear and meaningful values rather than using abbreviated terms.
*/

-- Using CTE
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)

WITH duplicates AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS record_flag
	FROM bronze.crm_cust_info
),
unique_primary_keys AS (
	SELECT
		*
	FROM duplicates
	WHERE
		record_flag = 1 
)

SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
FROM unique_primary_keys;



SELECT * FROM silver.crm_cust_info LIMIT 50;

-- checking duplicates
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1;

-- checking unwanted space
SELECT
	cst_lastname
FROM silver.crm_cust_info
WHERE
	cst_lastname <> TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT 
	DISTINCT(cst_gndr)
FROM silver.crm_cust_info;
-- Check for Invalid Dates
-- 1. Negative number or zeros cant be cast to date
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt <= 0;

-- 2. NULLIF() returns a NULL if the given values are equal
-- otherwise, it returns the firt expressions
SELECT
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt <= 0 OR
	LENGTH(sls_order_dt::text) != 8;

SELECT
	LENGTH(sls_order_dt::text)
FROM bronze.crm_sales_details;

-- 3. In some scenario, the length of the date must be 8.
-- Checking the column is the best way.
-- Checking for outliers by validating the boundaries
-- of the date range.

SELECT
	NULLIF(sls_due_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE
	sls_due_dt <= 0
OR	LENGTH(sls_due_dt::text) != 8
OR	sls_due_dt >= 20500101
OR	sls_due_dt <= 18000101;

-- Checking for Dates
-- 1. Order date must be less than or always earlier than the
-- shipping date or due date

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
OR	sls_order_dt > sls_due_dt;


-- Check Data Consistency: Between Sales, Quantity, and Price
-- Sales = Quanity * Price
-- Value must not be NULL, zero, or negative.

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales != (sls_quantity * sls_price)
OR 	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR 	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;


-- Talk with the expert
-- Solution 1. Data Issues will be fixed direct in source system
-- Solution 2. Data Issues has to be fixed in data warehouse.
	-- Ask with the expert to resolve these issues.
	-- It all depends on the different rules.
		-- Example Rules:
			-- If Sales is negative, zero, or null, derive it using Quantity and Price.
			-- If Price is zero or null, calculate it using Sales and Quantity
			-- If Price is negative, convert it to a positive value.

SELECT
	sls_sales AS old_sales,
	sls_quantity,
	sls_price AS old_price,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales != (sls_quantity * sls_price)
OR 	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR 	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;





SELECT * FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_cust_info;




-- Finalizing crm_sales_details cleaning and transformation
-- Using CTE

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)

WITH cleaned_crm_sales_details AS (
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN	sls_order_dt <= 0 
			OR 		LENGTH(sls_order_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_order_dt::text, 'YYYYMMDD')
		END AS sls_order_dt,
		CASE
			WHEN	sls_ship_dt <= 0 
			OR 		LENGTH(sls_ship_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_ship_dt::text, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE
			WHEN	sls_due_dt <= 0 
			OR 		LENGTH(sls_due_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_due_dt::text, 'YYYYMMDD')
		END AS sls_due_dt,
		CASE
			WHEN 	sls_sales != sls_quantity * ABS(sls_price)
			OR		sls_sales <= 0
			OR		sls_sales IS NULL
			THEN	sls_quantity * ABS(sls_price)
			ELSE	sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN	sls_price <= 0
			OR		sls_price IS NULL
			THEN	sls_sales / NULLIF(sls_quantity, 0)
			ELSE	sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
)

SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM cleaned_crm_sales_details;
	



SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM cleaned_crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;



-- Check Data Consistency
SELECT 
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;

SELECT * FROM silver.crm_sales_details;



SELECT
	pg_typeof(sls_sales),
	pg_typeof(sls_quantity),
	pg_typeof(sls_price),
	pg_typeof(sls_quantity * sls_price),
	pg_typeof(sls_sales / NULLIF(sls_quantity, 0))
FROM bronze.crm_sales_details
LIMIT 1;



SELECT
	ABS(sls_sales) AS old_sls_sales,
	ABS(sls_quantity) AS old_sls_quantity,
	ABS(sls_price) AS old_sls_price,
	CASE
		WHEN	sls_sales <= 0
		OR		sls_sales  IS NULL
		OR		ABS(sls_sales) != ABS(sls_quantity * sls_price)
		THEN	ABS(sls_quantity * sls_price)
		ELSE	sls_sales
	END AS sls_sales
FROM bronze.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;













	SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details
		WHERE
			sls_sales != sls_quantity * sls_price
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;










/*

	TESTING
	
*/ 








-- Clean by Column

SELECT * FROM bronze.crm_sales_details;

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM(
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN	sls_order_dt <= 0
		OR		LENGTH(sls_order_dt::text) != 8
		THEN	NULL
		ELSE	to_date(sls_order_dt::text, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE
		WHEN	sls_ship_dt <= 0
		OR		LENGTH(sls_ship_dt::text) != 8
		THEN	NULL
		ELSE	to_date(sls_ship_dt::text, 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE
		WHEN	sls_due_dt <= 0
		OR		LENGTH(sls_due_dt::text) != 8
		THEN	NULL
		ELSE	to_date(sls_due_dt::text, 'YYYYMMDD')
	END	AS sls_due_dt,
	CASE
		WHEN	sls_sales != sls_quantity * ABS(sls_price)
		OR		sls_sales <= 0
		OR		sls_sales IS NULL
		THEN	sls_quantity * ABS(sls_price)
		ELSE	sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN	sls_price <= 0
		OR		sls_price IS NULL
		THEN	ABS(sls_sales) / NULLIF(sls_quantity, 0)
		ELSE	sls_price
	END AS sls_price
FROM bronze.crm_sales_details
) AS test
WHERE
	sls_sales != sls_quantity * sls_price
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL;




SELECT * FROM silver.crm_cust_info;


			-- DATES
-- 1. Checking the Order dates
SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
OR	sls_order_dt > sls_due_dt;

-- 2. Formatting the date
SELECT 
	sls_due_dt,
	CASE
		WHEN	sls_due_dt <= 0
		OR		LENGTH(sls_due_dt::text) != 8
		THEN	NULL
		ELSE	to_date(sls_due_dt::text, 'YYYYMMDD')
	END AS sls_due_dt
FROM bronze.crm_sales_details
WHERE	sls_due_dt <= 0
OR		LENGTH(sls_due_dt::text) != 8;



			-- Data Consistency
-- Checking one-by-one
SELECT
	sls_sales,
	sls_quantity,
	sls_price,
	CASE
		WHEN	sls_price != sls_sales / NULLIF(sls_quantity, 0)
		OR		sls_price <= 0
		OR		sls_price IS NULL
		THEN	ABS(sls_sales) / NULLIF(sls_quantity, 0)
		ELSE	sls_price
	END AS n_sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales != sls_quantity * ABS(sls_price)
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL;


SELECT 
	sls_sales
FROM bronze.crm_sales_details
WHERE
	sls_sales <= 0;

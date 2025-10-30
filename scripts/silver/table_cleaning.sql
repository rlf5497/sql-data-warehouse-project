-- Data Cleansing and Transformation


-- CRM_PRD_INFO Table
-- 1. Check of Duplicate and NULL primary Key
SELECT * FROM bronze.crm_prd_info;

SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1
OR	prd_id IS NULL;

-- 2. Cost is not less than 0
SELECT
	prd_cost
FROM  silver.crm_prd_info
WHERE
	prd_cost < 0
OR 	prd_cost IS NULL;

-- 3. Data Normalization
SELECT DISTINCT
	prd_line,
	CASE	UPPER(TRIM(prd_line))
		WHEN	'M'		THEN 	'Mountain'
		WHEN	'R'		THEN	'Road'
		WHEN	'S'		THEN	'Other Sales'
		WHEN	'T'		THEN	'Touring'
		ELSE	'n/a'
	END AS prd_line
FROM silver.crm_prd_info;


-- FINAL QUERY (silver.crm_prd_info)
SELECT
	prd_id,
	REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
	SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
	TRIM(prd_nm) AS prd_nm
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE	UPPER(TRIM(prd_line))
		WHEN	'M'		THEN	'Mountain'
		WHEN	'R'		THEN	'Road'
		WHEN	'S'		THEN	'Other Sales'
		WHEN	'T'		THEN	'Touring'
		ELSE	'n/a'
	END AS prd_line,
	prd_start_dt::DATE AS prd_start_dt,
	(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 day')::DATE AS prd_end_dt
FROM bronze.crm_prd_info;




-- CRM_SALES_DETAILS
-- 1. Check primary key for null or duplicate values
SELECT * FROM bronze.crm_sales_details;

-- 2. Check for invalid dates
SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM silver.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
OR	sls_order_dt > sls_due_dt
OR	LENGTH(sls_order_dt::text) < 8
OR	LENGTH(sls_ship_dt::text) < 8
OR	LENGTH(sls_due_dt::text) < 8;

-- Date Standardization
SELECT
	sls_order_dt,
	CASE
		WHEN	LENGTH(sls_order_dt::text) != 8
		OR		sls_order_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_order_dt::text, 'YYYYMMDD')
	END AS sls_order_dt_updated,
	sls_ship_dt,
	CASE
		WHEN	LENGTH(sls_ship_dt::text) != 8
		OR		sls_ship_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
	END AS sls_ship_dt_updated,
	sls_due_dt,
	CASE
		WHEN	LENGTH(sls_due_dt::text) != 8
		OR		sls_due_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_due_dt::text, 'YYYYMMDD')
	END AS sls_due_dt_updated
FROM bronze.crm_sales_details;


-- Business Rules: Sales = Quantity * Price
-- SALES must not be Negative, Zeroes, NULLS


-- SALES
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales <= 0
OR	sls_sales IS NULL;

-- QUANTITY
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE
	sls_quantity <= 0
OR	sls_quantity IS NULL;

-- PRICE
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE
	sls_price <= 0
OR	sls_price IS NULL;


SELECT *
FROM (
SELECT
	sls_sales,
	sls_quantity,
	sls_price,
	CASE
		WHEN	sls_sales != sls_quantity * ABS(sls_price)
		OR		sls_sales <= 0
		OR		sls_sales IS NULL
		THEN	sls_quantity * ABS(sls_price)
		ELSE	sls_sales
	END AS sls_sales_check,
	CASE
		WHEN	sls_price <= 0
		OR		sls_price IS NULL
		THEN	ABS(sls_sales) / NULLIF(sls_quantity, 0)
		ELSE	sls_price
	END AS sls_price_check
FROM bronze.crm_sales_details
) AS sq
WHERE
	sls_sales != sls_quantity * ABS(sls_price)
OR	sls_sales <= 0
OR	sls_sales IS NULL
OR	sls_quantity <= 0
OR	sls_quantity IS NULL
OR	sls_price <= 0
OR	sls_price IS NULL;


-- FINAL QUERY (crm_sales_details)
WITH checking AS (
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN	LENGTH(sls_order_dt::TEXT) != 8
		OR		sls_order_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE
		WHEN	LENGTH(sls_ship_dt::TEXT) != 8
		OR		sls_ship_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE
		WHEN	LENGTH(sls_due_dt::TEXT) != 8
		OR		sls_due_dt <= 0
		THEN	NULL
		ELSE	TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	END AS sls_due_dt,
		CASE
		WHEN	sls_sales != sls_quantity * ABS(sls_price)
		OR		sls_sales <= 0
		OR		sls_sales IS NULL
		THEN	sls_quantity * ABS(sls_price)
		ELSE	sls_sales
	END AS sls_sales,
	ABS(sls_quantity) AS sls_quantity,
	CASE
		WHEN	sls_price <= 0
		OR		sls_price IS NULL
		THEN	ABS(sls_sales) / NULLIF(sls_quantity, 0)
		ELSE	sls_price
	END AS sls_price
FROM silver.crm_sales_details
)

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE
	sls_sales != sls_quantity * ABS(sls_price)
OR	sls_sales <= 0
OR	sls_sales IS NULL
OR	sls_quantity <= 0
OR	sls_quantity IS NULL
OR	sls_price <= 0
OR	sls_price IS NULL;





-- ERP_CUST_AZ12
-- Data Quality Checks
-- 1. Nulls and Duplicates for the Primary Key
SELECT *
FROM bronze.erp_cust_az12;

SELECT cst_key
FROM silver.crm_cust_info;

-- 2. check for unwanted spaces
SELECT
	cid
FROM bronze.erp_cust_az12
WHERE
	cid <> TRIM(cid);

-- 3. Check invalid Dates, FUTURE Dates or Very Low Dates
SELECT
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE
	bdate >= CURRENT_DATE;

-- 4. Data Normalization: 
SELECT DISTINCT
	gen,
	CASE
		WHEN	UPPER(TRIM(gen)) IN ('M', 'MALE')		THEN 'Male'
		WHEN	UPPER(TRIM(gen)) IN ('F', 'FEMALE')	THEN 'Female'	
		ELSE	'n/a'
	END AS tst
FROM bronze.erp_cust_az12;

-- FINAL QUERY erp_cust_az12
SELECT
	CASE
		WHEN	TRIM(cid)	ILIKE 	'nas%'
		THEN	SUBSTRING(TRIM(cid), 4, LENGTH(TRIM(cid)))
		ELSE	TRIM(cid)
	END AS cid,
	CASE
		WHEN	bdate >= CURRENT_DATE
		THEN	NULL
		ELSE	bdate
	END AS bdate,
	CASE
		WHEN	UPPER(TRIM(gen)) IN ('M', 'MALE')		THEN 'Male'
		WHEN	UPPER(TRIM(gen)) IN ('F', 'FEMALE') 	THEN 'Female'
		ELSE	'n/a'
	END AS gen
FROM bronze.erp_cust_az12;



-- ERP_LOC_A101
-- Data Quality Checks
SELECT * FROM bronze.erp_loc_a101;

-- 1. check for unwated space
SELECT DISTINCT
	cntry,
	cntry_clean
FROM (
SELECT
	cid,
	REPLACE(TRIM(cid), '-', '') AS cid_clean,
	cntry,
	CASE
		WHEN	UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES')	THEN 'United States'
		WHEN	TRIM(cntry) = '' OR cntry IS NULL	THEN 'n/a'
		WHEN	UPPER(TRIM(cntry)) = 'DE'	THEN 'Germany'
		ELSE	TRIM(cntry)
	END AS cntry_clean
FROM bronze.erp_loc_a101
);

 
-- 2. Data Normalization/Standardization and Consistency for low cardinality
SELECT DISTINCT
	cntry
FROM bronze.erp_loc_a101;

-- Final Query erp_loc_a101
SELECT
	REPLACE(TRIM(cid), '-', '') AS cid,
	CASE
		WHEN	UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES')	THEN 'United States'
		WHEN	TRIM(cntry) = '' OR cntry IS NULL	THEN 'n/a'
		WHEN	UPPER(TRIM(cntry)) = 'DE'	THEN 'Germany'
		ELSE	TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;
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





-- 
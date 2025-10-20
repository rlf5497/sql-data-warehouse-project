SELECT
	cid,
	TRIM(SUBSTRING(cid, 4, LENGTH(cid))) AS checking
FROM bronze.erp_cust_az12;

SELECT *
FROM silver.crm_cust_info;


-- 1. Equal the cst_key to cid
-- 2. Identify out-of-range dates

SELECT 	DISTINCT
	cid,
	bdate,
	gen
FROM (
SELECT
	CASE
		WHEN	cid ILIKE 'nas%'
		THEN	SUBSTRING(TRIM(cid), 4, LENGTH(cid))
		ELSE	cid
	END AS cid,
	CASE
		WHEN	bdate > CURRENT_DATE
		THEN	NULL
		ELSE	bdate
	END AS bdate,
	CASE
		WHEN	UPPER(TRIM(gen)) IN ('F', 'FEMALE')
		THEN	'Female'
		WHEN	UPPER(TRIM(gen)) IN ('M', 'MALE')
		THEN	'Male'
		ELSE	'n/a'
	END AS gen
FROM bronze.erp_cust_az12
) AS sq;
	
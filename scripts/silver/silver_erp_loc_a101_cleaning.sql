SELECT *
FROM bronze.erp_loc_a101;

-- Replace '-' to match cst_key in silver.crm_cust_info
SELECT
	REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101
WHERE
	REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info);	


-- Low cardinality, data normalization & standardization
SELECT DISTINCT
	cntry AS old_cntry,
	CASE
		WHEN	TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN	TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN	TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE	TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
ORDER BY
	old_cntry;


-- FINAL
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT DISTINCT
	cid,
	cntry
FROM (
	SELECT
		REPLACE(TRIM(cid), '-', '') AS cid,
		CASE
			WHEN	TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN	TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN	TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE	TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101
) AS sq;

-- Checking

SELECT
	cid,
	cntry
FROM silver.erp_loc_a101
WHERE
	cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

SELECT
	DISTINCT cntry
FROM silver.erp_loc_a101;
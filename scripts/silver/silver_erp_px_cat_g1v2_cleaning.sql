-- Cleaning
SELECT *
FROM bronze.erp_px_cat_g1v2

-- Check category id, connected to prd_info table
SELECT
	id
FROM bronze.erp_px_cat_g1v2
WHERE
	id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

-- check unwanted space
SELECT
	maintenance
FROM bronze.erp_px_cat_g1v2
WHERE
	maintenance <> TRIM(maintenance);

-- Low cardinality
SELECT 
	DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;


SELECT * FROM silver.erp_px_cat_g1v2;
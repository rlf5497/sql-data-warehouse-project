-- Quality Checks


-- Checking silver.crm_cust_info
--- Check for Nulls or Duplciates in Primary key
-- Expectation: No Results
SELECT
	cst_id,
	COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1
OR	cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	cst_key
FROM silver.crm_cust_info
WHERE
	cst_key <> TRIM(cst_key);

-- Data Standardization & Consistenncy
SELECT DISTINCT
	cst_marital_status
FROM silver.crm_cust_info;



-- Checking silver.crm_prd_info
-- Check for NULLs or Duplicates in Primary Key
SELECT
	prd_id,
	COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1
OR	prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE
	prd_nm <> TRIM(prd_nm);

-- Check for NULLs or Negative Value in Cost
-- Expectation: No Results
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE
	prd_cost < 0
OR	prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Dates Orders (Start Date > End Date)
-- Expection: No Results
SELECT
	*
FROM silver.crm_prd_info
WHERE
	prd_start_dt > prd_end_dt;


-- Checking silver.crm_sales_details
-- Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT
	*
FROM bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
OR	sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE
	sls_sales <> sls_quantity * sls_price
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL;



-- Checking silver.erp_cust_az12
-- Indentify Out-of-Range Dates
-- Expectation: No Future Dates
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE
	bdate > CURRENT_DATE;

-- Data Standardization & Consistency
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12;



-- Checking silver.erp_loc_a101
-- Data Standardization & Consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101;



-- Checking silver.erp_px_cat_g1v2
-- Check for Unwated Space
-- Expectation: No Results
SELECT
	*
FROM silver.erp_px_cat_g1v2
WHERE
	cat <> TRIM(cat)
OR	subcat <> TRIM(subcat)
OR	maintenance <> TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2;
/*
==========================================================================================
Script: load_silver.sql
==========================================================================================
Purpose:
	Defines the procedure to load and transform data from the Bronze (raw) layer
	into the Silver (cleaned and standardized) layer within the data warehouse.

	This script performs the following:
		- Truncates Silver tables before reloading
		- Cleans and standardizes Bronze data
		- Inserts transformed data into the Silver schema
		- Logs load duration for each table for performance tracking

Details:
	- Architecture: Medallion (Bronze -> Silver -> Gold)
	- Layers:
		• Bronze: Raw extracted data
		• Silver: Cleaned and conformed data ready for analytical enrichment
	- Execution:
		CALL silver.load_silver():
==========================================================================================
*/



CALL silver.load_silver();



CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time			TIMESTAMP;
	end_time			TIMESTAMP;
	loaded_count		INTEGER;
	batch_start_time	TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '====================================================';



    ---------------------------------------------------------------------------
    -- CRM Tables
    ---------------------------------------------------------------------------
    RAISE NOTICE '----------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------------------';
	


	-- ==========================================================
	-- silver.crm_cust_info
	-- Transformation:
	--	• Removes duplicate customer IDs, keeps latest by date
	--	• Trims whitespace in names and attributes
	--	• Converts marital status and gender codes into full words
	-- ==========================================================
	start_time := clock_timestamp();
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

	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE	UPPER(TRIM(cst_marital_status))
			WHEN	'S'		THEN	'Single'
			WHEN	'M'		THEN	'Married'
			ELSE	'n/a'
		END AS cst_marital_status,
		CASE	UPPER(TRIM(cst_gndr))
			WHEN	'M'		THEN	'Male'
			WHEN	'F'		THEN	'Female'
			ELSE	'n/a'
		END AS cst_gndr,
		cst_create_date
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS recent_flag
		FROM bronze.crm_cust_info
		WHERE
			cst_id IS NOT NULL
	) AS sq
	WHERE
		recent_flag =  1;
	SELECT COUNT(*) INTO loaded_count FROM silver.crm_cust_info;
	end_time := clock_timestamp();
	RAISE NOTICE '>> crm_cust_info loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;



	-- ==========================================================
	-- silver.crm_prd_info
	-- Transformation:
	--	• Extracts product category ID from prd_key
	--	• Maps product line codes (M, R, S, T) to full names
	--	• Handle NULL or invalid costs
	--	• Calcualtes prd_end_dt using LEAD() window function
	-- ==========================================================
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	SELECT
		prd_id,
		REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
		SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
		TRIM(prd_nm) AS prd_nm,
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
	SELECT COUNT(*) INTO loaded_count FROM silver.crm_prd_info;
	end_time := clock_timestamp();
	RAISE NOTICE '>> crm_prd_info loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;
		

	
	-- ==========================================================
	-- silver.crm_sales_details
	-- Transformation:
	--	• Converts integer date fields (YYYYMMDD) into DATE type
	--	• Fixes inconsistent sales values and ensure price positivity
	--	• Recalculates missing or invalid sales from quantity * price
	-- ==========================================================
	start_time := clock_timestamp();
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
	FROM bronze.crm_sales_details;
	SELECT COUNT(*) INTO loaded_count FROM silver.crm_sales_details;	
	end_time := clock_timestamp();
	RAISE NOTICE '>> crm_sales_details loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;


	
    ---------------------------------------------------------------------------
    -- ERP Tables
    ---------------------------------------------------------------------------
    RAISE NOTICE '----------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------------------';



	-- ==========================================================
	-- silver.erp_cust_az12
	-- Transformation:
	--	• Removes 'NAS' prefix from CID values
	--	• Nullifies future birthdates
	--	• Standardizes gender values
	-- ==========================================================
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)

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
	SELECT COUNT(*) INTO loaded_count FROM silver.erp_cust_az12;
	end_time := clock_timestamp();
	RAISE NOTICE '>> erp_cust_az12 loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;


	-- ==========================================================
	-- silver.erp_loc_a101
	-- Transformation:
	--	• Removes hyphens from CID
	--	• Standardizes country names and fills missing values
	-- ==========================================================
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	
	SELECT
		REPLACE(TRIM(cid), '-', '') AS cid,
		CASE
			WHEN	UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES')	THEN 'United States'
			WHEN	TRIM(cntry) = '' OR cntry IS NULL	THEN 'n/a'
			WHEN	UPPER(TRIM(cntry)) = 'DE'	THEN 'Germany'
			ELSE	TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;
	SELECT COUNT(*) INTO loaded_count FROM silver.erp_loc_a101;
	end_time := clock_timestamp();
	RAISE NOTICE '>> erp_loc_a101 loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;

		

	-- ==========================================================
	-- silver.erp_px_cat_g1v2
	-- Transformation:
	--	• Removes trailing spaces from all string columns
	-- ==========================================================
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)

	SELECT 
		id,
		TRIM(cat) AS cat,
		TRIM(subcat) AS subcat,
		TRIM(maintenance) AS maintenance
	FROM bronze.erp_px_cat_g1v2;
	SELECT COUNT(*) INTO loaded_count FROM silver.erp_px_cat_g1v2;
	end_time := clock_timestamp();
	RAISE NOTICE '>> erp_px_cat_g1v2 loaded in % seconds (% rows)', 
		EXTRACT(epoch FROM end_time - start_time), loaded_count;



	-------------------------------------------------------------------------
    -- Summary
    -------------------------------------------------------------------------
    RAISE NOTICE '===================================================';
    RAISE NOTICE '	Loading Silver Layer is Completed';
    RAISE NOTICE '	Total Load Duration: % seconds',
        EXTRACT(epoch FROM clock_timestamp() - batch_start_time);
    RAISE NOTICE '===================================================';


	
EXCEPTION
	WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
        RAISE;  -- rethrow for debugging
END;
$$;

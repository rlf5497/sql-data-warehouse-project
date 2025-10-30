CALL silver.load_silver();


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time			TIMESTAMP;
	end_time			TIMESTAMP;
	batch_start_time	TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '====================================================';

    ---------------------------------------------------------------------------
    -- CRM Tables
    ---------------------------------------------------------------------------
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '====================================================';


	-- Loading silver.crm_cust_info
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
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
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);


	-- Loading silver.crm_prd_info
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
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
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);
		
	
	-- Loading silver.crm_sales_details
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);
	

	-- Loading silver.erp_cust_az12
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
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
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);
	
EXCEPTION
	WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
        RAISE;  -- rethrow for debugging
END;
$$;


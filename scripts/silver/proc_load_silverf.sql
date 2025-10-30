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
	
EXCEPTION
	WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
        RAISE;  -- rethrow for debugging
END;
$$;

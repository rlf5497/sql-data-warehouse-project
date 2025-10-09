/*
============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================================
Script Purpose:
	Loads CSV data into the 'bronze' schema tables.
	- Truncates existing data to avoid duplicates
	- Uses PostgreSQL's COPY command to import data efficiently
============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time 	TIMESTAMP;
	end_time	TIMESTAMP:
	batch_start_time	TIMESTAMP := clock_timestamp();
BEGIN
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '===================================================='


	---------------------------------------------------------------------------
	-- CRM Tables
	---------------------------------------------------------------------------
	RAISE NOTICE '----------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------------------';

	-- crm_cust_info
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	EXECUTE format($f$
		COPY bronze.crm_cust_info
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv');
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	-- crm_prd_info
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	EXECUTE format($f$
		COPY bronze.crm_prd_info
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv');
	end_time := clock_timestamp()
	RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	-- crm_sales_details
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	EXECUTE format($f$
		COPY bronze.crm_sales_details
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv');
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	---------------------------------------------------------------------------
	-- ERP Tables
	---------------------------------------------------------------------------
	RAISE NOTICE '----------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------------------';


	-- erp_loc_a101
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	EXECUTE format($f$
		COPY bronze.erp_loc_a101
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv');
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duratin: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	-- erp_cust_az12
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	EXECUTE format($f$
		COPY bronze.erp_cust_az12
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv');
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	-- erp_px_cat_g1v2
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Loading Data Into: bronze.erp_px_cat_g1v2';
	EXECUTE format($f$
		COPY bronze.erp_px_cat_g1v2
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, 'C:\Users\Reymart L. Felisilda\Desktop\Data Engineer\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv');
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM end_time - start_time);
	RAISE NOTICE '>> ----------------';


	-------------------------------------------------------------------------
    -- Summary
    -------------------------------------------------------------------------
	RAISE NOTICE '==============================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
	RAISE NOTICE '		- Total Load Duration: % seconds', extract(epoch FROM clock_timestamp() - batch_start_time);
	RAISE NOTICE '==============================================';	


EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '================================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE '================================================';
END;
$$;
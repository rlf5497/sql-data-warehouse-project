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

CREATE OR REPLACE PROCEDURE bronze.load_bronze(base_path TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
    loaded_count     INTEGER;
    batch_start_time TIMESTAMP := clock_timestamp();

    crm_cust_info_path TEXT := base_path || '\source_crm\cust_info.csv';
    crm_prd_info_path  TEXT := base_path || '\source_crm\prd_info.csv';
    crm_sales_details_path TEXT := base_path || '\source_crm\sales_details.csv';
    erp_loc_a101_path  TEXT := base_path || '\source_erp\loc_a101.csv';
    erp_cust_az12_path TEXT := base_path || '\source_erp\cust_az12.csv';
    erp_px_cat_g1v2_path TEXT := base_path || '\source_erp\px_cat_g1v2.csv';
BEGIN
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading Bronze Layer from Base Path: %', base_path;
    RAISE NOTICE '====================================================';

    ---------------------------------------------------------------------------
    -- CRM Tables
    ---------------------------------------------------------------------------
    RAISE NOTICE '----------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE format($f$
        COPY bronze.crm_cust_info
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, crm_cust_info_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.crm_cust_info;
    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_cust_info loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE format($f$
        COPY bronze.crm_prd_info
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, crm_prd_info_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.crm_prd_info;
    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_prd_info loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE format($f$
        COPY bronze.crm_sales_details
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, crm_sales_details_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.crm_sales_details;
    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_sales_details loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;


    ---------------------------------------------------------------------------
    -- ERP Tables
    ---------------------------------------------------------------------------
    RAISE NOTICE '----------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------------------';

    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE format($f$
        COPY bronze.erp_loc_a101
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, erp_loc_a101_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.erp_loc_a101;
    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_loc_a101 loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE format($f$
        COPY bronze.erp_cust_az12
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, erp_cust_az12_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.erp_cust_az12;
    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_cust_az12 loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE format($f$
        COPY bronze.erp_px_cat_g1v2
        FROM %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, erp_px_cat_g1v2_path);
    SELECT COUNT(*) INTO loaded_count FROM bronze.erp_px_cat_g1v2;
    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_px_cat_g1v2 loaded in % seconds (% rows)',
        extract(epoch FROM end_time - start_time), loaded_count;


    -------------------------------------------------------------------------
    -- Summary
    -------------------------------------------------------------------------
    RAISE NOTICE '===================================================';
    RAISE NOTICE '	Loading Bronze Layer is Completed';
    RAISE NOTICE '	Total Load Duration: % seconds',
        extract(epoch FROM clock_timestamp() - batch_start_time);
    RAISE NOTICE '===================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
        RAISE;  -- rethrow for debugging
END;
$$;


CALL bronze.load_bronze('C:\data-engineering-projects\sql\data-warehouse\sql-data-warehouse-project\datasets');

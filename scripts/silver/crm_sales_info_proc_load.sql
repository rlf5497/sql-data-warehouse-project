WITH sales_details AS (
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR 
				LENGTH(sls_order_dt::text) != 8 THEN NULL
			ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR
				LENGTH(sls_ship_dt::text) != 8 THEN NULL
			ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR
				LENGTH(sls_due_dt::text) != 8 THEN NULL
			ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
		END AS sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	FROM bronze.crm_sales_details
)

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS new_sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price <= 0 OR sls_price IS NULL THEN (sls_sales / NULLIF(sls_quantity, 0))
		ELSE sls_price
	END AS new_sls_price
FROM sales_details;




























SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM (
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN	sls_order_dt <= 0 
			OR 		LENGTH(sls_order_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_order_dt::text, 'YYYYMMDD')
		END AS sls_order_dt,
		CASE
			WHEN	sls_ship_dt <= 0 
			OR 		LENGTH(sls_ship_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_ship_dt::text, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE
			WHEN	sls_due_dt <= 0 
			OR 		LENGTH(sls_due_dt::text) != 8 
			THEN 	NULL
			ELSE 	to_date(sls_due_dt::text, 'YYYYMMDD')
		END AS sls_due_dt,
		CASE
			WHEN 	ABS(sls_sales) != ABS(sls_quantity * sls_price)
			OR		sls_sales <= 0
			OR		sls_sales IS NULL
			THEN	ABS(sls_quantity * sls_price)
			ELSE	sls_sales
		END AS sls_sales,
		ABS(sls_quantity) AS sls_quantity,
		CASE
			WHEN	ABS(sls_price) != ABS(sls_sales / NULLIF(sls_quantity, 0))
			OR		sls_price <= 0
			OR		sls_price IS NULL
			THEN	ABS(sls_sales / NULLIF(sls_quantity, 0))
			ELSE	sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
) AS sq
WHERE
	ABS(sls_sales) != ABS(sls_quantity * sls_price)
OR	sls_sales IS NULL
OR	sls_quantity IS NULL
OR	sls_price IS NULL
OR	sls_sales <= 0
OR	sls_quantity <= 0
OR	sls_price <= 0;
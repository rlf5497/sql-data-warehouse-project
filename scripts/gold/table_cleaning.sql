-- Data Integration

-- Asked the Source System EXPERT which is the Master Table for these values.
-- NULLs often come from JOINED TABLES
-- NULL will appear if SQL finds no match.
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE
		WHEN	ci.cst_gndr != 'n/a'	THEN	ci.cst_gndr
		ELSE	COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	   ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
	   ON ci.cst_key = la.cid
ORDER BY
	1, 2;

-- Data Integration, integrating two different source system in One. 
-- This is exactly what we call data integration
-- Why we tried to get data from different source system in order to get rich
-- information in the data warehouse.

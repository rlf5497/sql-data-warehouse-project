-- Table: silver.crm_sales_details

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE IF NOT EXISTS silver.crm_sales_details
(
    sls_ord_num character varying(50) COLLATE pg_catalog."default",
    sls_prd_key character varying(50) COLLATE pg_catalog."default",
    sls_cust_id integer,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales integer,
    sls_quantity integer,
    sls_price integer,
    dwh_create_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS silver.crm_sales_details
    OWNER to postgres;
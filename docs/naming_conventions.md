# Naming Conventions
This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.



## Table of Contents
1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   -  [Bronze Rules](#bronze-rules)
   -  [Silver Rules](#silver-rules)
   -  [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
4. [View Naming Convetions](#view-naming-conventions)

---

## General Principles
- Use **snake_case** for all names (e.g., 'customer_id', 'order_date').
- Avoid spaces or special characters in names.
- Be descriptive but concise
- Prefix or suffix consistently to indicate meaning (e.g., '_id'. '_key', '_dt').
- Maintain uniform naming across all layers (Bronze → Silver → Gold).

---

## Table Naming Conventions

| Layer  | Schema Name | Table/View Prefix | Example              | Description |
|--------|--------------|------------------|----------------------|-------------|
| Bronze | `bronze`     | `raw_` or source name | `bronze.crm_cust_info` | Raw, unprocessed data loaded from source systems |
| Silver | `silver`     | same as source | `silver.crm_cust_info` | Cleaned, validated, and transformed tables |
| Gold   | `gold`       | `dim_` / `fact_` | `gold.dim_customers` | Final business-ready views (Star Schema) |

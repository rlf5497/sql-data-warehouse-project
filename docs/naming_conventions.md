# Naming Conventions
This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.



## Table of Contents
1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   -  [Bronze Rules](#bronze-rules)
   -  [Silver Rules](#silver-rules)
   -  [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Key](#surrogate-keys)
   - [Technical Columms](#technical-columns)
4. [Stored Procedures](#stored-procedures)

---

## General Principles
- **Naming Style:** Use `snake_case` (lowercase letters with underscores `_`).
- **Language:** Use **English** for all objects names.
- **Clarity:** Names should be **descriptive and meaningful**, avoiding abbreviations unless well-known.
- **Reserved Words:** Do **not** use SQL reserved keywords as identifiers.
- **Consistency:** Follow the same naming logic across all schemas and layers.

---

## Table Naming Conventions

### Bronze Rules
- Tables in the Bronze Layer mirror the **raw data from source systems**.
  
- Use the naming pattern: `<sourcesystem>_<entity>`
  
     - `<source_system>` → Source system name (e.g., `crm`, `erp`)
       
     - `<entity>` → Original table name from the source system

   **Examples:**
     - `crm_customer_info` → Customer information from the CRM system
     - `erp_product_master` → Product details from the ERP system

---

### Silver Rules
- Tables in the Silver layer are ***cleaned and transformed*** versions of the Bronze tables.
- Maintain the same base name for traceability.
- Use the same pattern: `<sourcesystem>_<entity>`
  
  **Examples:**
     - `crm_customer_info` → Cleaned customer information
     - `erp_product_master` → Transformed product data

---

### Gold Rules
- Tables in the Gold layer represent **business-ready data** using **Star Schema** (dimension and fact tables).
- Use the pattern: `<category>_<entity>`
     - `<category>` → Table role (e.g., `dim` for dimension, `fact` for fact)
     - `<entity>` → Business concept (e.g., `customers`, `sales`)

  **Examples:**
     - `dim_customers` → Dimension table for customer data
     - `dim_products` → Dimension table for product data
     - `fact_sales` → Fact table for sales transactions

#### Glossary of Category Patterns
| Pattern  | Meaning           | Example(s)                        |
|-----------|------------------|-----------------------------------|
| `dim_`    | Dimension table  | `dim_customers`, `dim_products`   |
| `fact_`   | Fact table       | `fact_sales`                      |
| `report_` | Report table     | `report_sales_monthly`, `report_kpi_summary` |

---

## Column Naming Conventions

### Surrogate Keys
- All surrogate (primary) keys in dimension tables must use the suffix `_key`.
- `<table_name>_key`
     - `<table_name>` → The entity or dimension name
     - `_key` → Indicates that it is a surrogate key

  **Examples:**
     - `customer_key` → Surrogate key in dim_customers
     - `product_key` → Surrogate key in dim_products

---

### Technical Columns
- Technical or metadata columns track system-related information.
- Use the prefix `dwh_` to differentiate them from business data.
- `dwh_<column_name>`
     - `dwh` → Prefix for system-managed metadata
     - `<column_name>` → Describes the column's purpose

  **Examples:**
     - `dwh_load_date` → The date the record was loaded into the warehouse
     - `dwh_modified_by` → Username or process that updated the record
     - `dwh_source` → Indicates the data source of the record

---

## Stored Procedures
- Stored procedures handle the data loading for each Medallion layer.
- Use the naming pattern: `load_<layer>`
     - `<layer>` → The target layer (`bronze`, `silver`, `gold`)

  **Examples:**
     - `load_bronze` → Loads raw data into the Bronze layer
     - `load_silver` → Cleans and transforms data into the Silver layer
     - `load_gold` → Builds dimension and fact views in the Gold layer
 
---

**Note:**
Following these conventions ensures **clarity, maintainability, and consistency** throughout the data warehouse lifecycle.
Each object name reflects its **purpose, data lineage, and layer association**, making collaboration and troubleshooting easier.

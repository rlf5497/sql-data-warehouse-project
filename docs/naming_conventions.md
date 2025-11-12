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
4. [Stored Procedure](#stored-procedure)

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

   **Example:**
     - `crm_customer_info` → Customer information from the CRM system
     - `erp_product_master` → Product details from the ERP system

---

### Silver Rules
- Tables in the Silver layer are ***cleaned and transformed*** versions of the Bronze tables.
- Maintain the same base name for traceability.
- Use the same pattern: `<sourcesystem>_<entity>`
  
  **Example:**
     - `crm_customer_info` → Cleaned customer information
     - `erp_product_master` → Transformed product data

---

### Gold Rules
- Tables in the Gold layer represent **business-ready data** using **Star Schema** (dimension and fact tables).
- Use the pattern: `<category>_<entity>`
     - `<category>` → Table role (e.g., `dim` for dimension, `fact` for fact)
     - `<entity>` → Business concept (e.g., `customers`, `sales`)

  **Example:**
     - `dim_customers` → Dimension table for customer data
     - `dim_products` → Dimension table for product data
     - `fact_sales` → Fact table for sales transactions

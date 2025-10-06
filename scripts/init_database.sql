/* 
================================================== 
Create Database and Schemas 
================================================== 
Script Purpose: 
	This script drops the existing 'datawarehouse' database (if it exists), 
	then recreates it, and creates three schemas: bronze, silver, and gold. 
	
	Warning: Running this script will permanently delete the 'datawarehouse' 
	database and all its data. Proceed with caution. */ 
	
-- Drop the 'data_warehouse' database if it exists. 
DROP DATABASE IF EXISTS data_warehouse; 

-- Create the 'data_warehouse' database 
CREATE DATABASE data_warehouse; 

-- Create Schemas for Data Warehouse Layers 
CREATE SCHEMA IF NOT EXISTS bronze; 
CREATE SCHEMA IF NOT EXISTS silver; 
CREATE SCHEMA IF NOT EXISTS gold;
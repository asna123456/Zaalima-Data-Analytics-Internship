-- Project: Consumer360 - Customer Analytics
-- Description: Data extraction and transformation in MySQL

-- Create the Database
CREATE DATABASE IF NOT EXISTS Zaalima_Retail_DB;
USE Zaalima_Retail_DB;

-- 1. Create Fact Table (Sales Data)
CREATE TABLE fact_sales AS
SELECT 
    o.order_id, 
    o.customer_id, 
    oi.product_id, 
    oi.price, 
    STR_TO_DATE(o.order_purchase_timestamp, '%Y-%m-%d %H:%i:%s') AS purchase_date
FROM raw_orders o
JOIN raw_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered';

-- 2. Create Customer Dimension Table
CREATE TABLE dim_customers AS
SELECT 
    customer_id, 
    customer_unique_id, 
    customer_city, 
    customer_state
FROM raw_customers;
   +-----------------------------+
| Tables_in_zaalima_retail_db |
+-----------------------------+
| dim_customers               |
| dim_products                |
| fact_sales                  |
| raw_customers               |
| raw_order_items             |
| raw_orders                  |
| raw_products                |
+-----------------------------+

-- 3. Create Product Dimension Table
CREATE TABLE dim_products AS
SELECT 
    product_id, 
    product_category_name
FROM raw_products;

-- Verify Tables
SHOW TABLES;
-- Check the final RFM Segmented Table structure
DESC rfm_segmented_data;

-- View top 10 customers with their RFM scores
SELECT 
    customer_id, 
    Recency, 
    Frequency, 
    Monetary, 
    R_Score, 
    F_Score, 
    M_Score
FROM rfm_segmented_data
LIMIT 10;
DESC rfm_segmented_data;
+-------------+--------+------+-----+---------+-------+
| Field       | Type   | Null | Key | Default | Extra |
+-------------+--------+------+-----+---------+-------+
| customer_id | text   | YES  |     | NULL    |       |
| Recency     | bigint | YES  |     | NULL    |       |
| Frequency   | bigint | YES  |     | NULL    |       |
| Monetary    | double | YES  |     | NULL    |       |
| R_Score     | text   | YES  |     | NULL    |       |
| F_Score     | text   | YES  |     | NULL    |       |
| M_Score     | text   | YES  |     | NULL    |       |
+-------------+--------+------+-----+---------+-------+
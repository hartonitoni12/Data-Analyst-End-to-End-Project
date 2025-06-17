/* PROJECT 2

================================================== EXPLORATORY DATA ANALYSIS ==================================================

Purpose:
  - Understand the data structure and content
  - Uncover underlying patterns and relationships
  - Formulate a foundation for further analysis

What We Do:
	- Database Exploration : Explore the objects of database
	- Dimension Exploration : Explore the Dimension of the datasets
	- Date Exploration : Explore the date type data
	- Measures Exploration : Explore the Measures that in datasets
	- Magnitude Analysis : Compare the measures value by categories
	- Ranking Analysis : Explore the N performance or bottom N performance
*/

/* ================================ DATABASE EXPLORATION ================================ */

-- View all objects in the database
SELECT * FROM INFORMATION_SCHEMA.TABLES

-- View all columns in dim_customers table
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_customers'

/* ================================ DIMENSION EXPLORATION ================================ */

-- Distinct countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers

-- Distinct product categories and names
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products

/* ================================ DATE EXPLORATION ================================ */

-- First and last order dates
SELECT MIN(order_date) AS firts_order, MAX(order_date) AS last_order FROM gold.fact_sales

-- Oldest and youngest customers
SELECT MIN(DATEDIFF(YEAR, birth_date, GETDATE())) AS youngest,
       MAX(DATEDIFF(YEAR, birth_date, GETDATE())) AS oldest
FROM gold.dim_customers

/* ================================ MEASURES EXPLORATION ================================ */

-- Total sales
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales

-- Total items sold
SELECT SUM(quantitiy) AS total_sold_item FROM gold.fact_sales

-- Average selling price
SELECT AVG(price) AS average_price FROM gold.fact_sales

-- Total number of orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales

-- Total number of products
SELECT COUNT(product_name) AS total_product FROM gold.dim_products
SELECT COUNT(DISTINCT product_name) AS total_product FROM gold.dim_products

-- Total number of customers
SELECT COUNT(customer_number) AS total_customers FROM gold.dim_customers
SELECT COUNT(DISTINCT customer_number) AS total_customers FROM gold.dim_customers

-- Customers who placed an order
SELECT COUNT(DISTINCT customer_key) AS customer_have_ordered FROM gold.fact_sales

-- Summary report of key business metrics
SELECT SUM(fs.sales_amount)              AS TotalSales,
       SUM(fs.quantitiy)                 AS TotalItemSold,
       AVG(fs.price)                     AS AVGprice,
       COUNT(DISTINCT fs.order_number)   AS NumberOfOrders,
       COUNT(DISTINCT dp.product_name)   AS NumberOfProduct,
       COUNT(DISTINCT dc.customer_number) AS NumberOfCustomer,
       COUNT(DISTINCT fs.customer_key)   AS NumberOfOrderedCustomes
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key

-- KPI summary using UNION ALL
SELECT 'Total Sales'               AS measure_name, SUM(sales_amount)             AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total Quantity'           AS measure_name, SUM(quantitiy)                AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Average Price'           AS measure_name, AVG(price)                    AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total Nr Orders'         AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total Nr Products'       AS measure_name, COUNT(DISTINCT product_name) AS measure_value FROM gold.dim_products UNION ALL
SELECT 'Total Nr Customers Ordering' AS measure_name, COUNT(customer_number)    AS measure_value FROM gold.dim_customers

/* ================================ MAGNITUDE ANALYSIS ================================ */

-- Total customers per country
SELECT country, COUNT(customer_number) AS total_customer FROM gold.dim_customers GROUP BY country ORDER BY total_customer DESC

-- Total customers per gender
SELECT gender, COUNT(customer_key) AS total_customer FROM gold.dim_customers GROUP BY gender ORDER BY total_customer DESC

-- Total products by category
SELECT category, COUNT(product_key) AS total_product FROM gold.dim_products GROUP BY category ORDER BY total_product DESC

-- Average cost by category
SELECT category, AVG(product_cost) AS avg_cost FROM gold.dim_products GROUP BY category ORDER BY avg_cost DESC

-- Revenue by country
SELECT dc.country, SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc ON fs.customer_key = dc.customer_key
GROUP BY dc.country ORDER BY total_revenue DESC

-- Top 10 customers by revenue
SELECT TOP 10 customer_key, SUM(sales_amount) AS tota_revenue FROM gold.fact_sales GROUP BY customer_key ORDER BY tota_revenue DESC

-- Total revenue by product
SELECT dp.product_name, SUM(sales_amount) AS total_sales
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name ORDER BY total_sales DESC

/* ================================ RANKING ANALYSIS ================================ */

-- Top 5 products by revenue
SELECT TOP 5 dp.product_name, SUM(sales_amount) AS total_sales
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name ORDER BY total_sales DESC

-- Top 5 products using RANK() function
SELECT TOP 5 product_name, total_sales, RANK() OVER (ORDER BY total_sales DESC) AS rank
FROM (
  SELECT dp.product_name, SUM(sales_amount) AS total_sales
  FROM gold.fact_sales AS fs
  LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key
  GROUP BY dp.product_name
) AS t ORDER BY rank ASC

-- Bottom 5 products by revenue
SELECT TOP 5 dp.product_name, SUM(sales_amount) AS total_sales
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name ORDER BY total_sales ASC

-- Bottom 5 products using RANK() function
SELECT dp.product_name, SUM(sales_amount) AS total_sales, RANK() OVER (ORDER BY SUM(sales_amount) ASC) AS rnk
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name ORDER BY total_sales ASC

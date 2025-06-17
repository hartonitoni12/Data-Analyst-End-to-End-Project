/* PROJECT 3
================================================== ADVANCE DATA ANALYSIS ==================================================
Purpose:
  - Gain Deeper, Hidden Insights
  - Understand the Trends

What We Do:
  - Change Over Time     : Track trends and identify seasonality
  - Cumulative Analysis  : Aggregate data progressively over time
  - Performance Analysis : Compare values against targets and past performance
  - Part to Whole        : Understand individual contributions to the whole
  - Data Segmentation    : Group data by range-based categories
  - Reporting            : Generate key metrics from fact and dimension tables
*/

/*===============================================
CHANGES OVER TIME / TRENDS
===============================================*/
-- Track how measures change over time (trends/seasonality)

USE DataWarehouse;

-- 1. Trend by year
SELECT
  YEAR(order_date)                  AS order_year,
  SUM(sales_amount)                AS total_sales,
  COUNT(order_number)              AS total_order,
  COUNT(DISTINCT customer_key)     AS number_of_customer,
  COUNT(DISTINCT product_key)      AS number_of_product
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);

-- 2. Trend by month
SELECT
  MONTH(order_date)                AS order_month,
  SUM(sales_amount)                AS total_sales,
  COUNT(order_number)              AS total_order,
  COUNT(DISTINCT customer_key)     AS number_of_customer,
  COUNT(DISTINCT product_key)      AS number_of_product
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date);

-- 3. Trend by year and month
SELECT
  YEAR(order_date)                 AS order_year,
  MONTH(order_date)                AS order_month,
  SUM(sales_amount)                AS total_sales,
  COUNT(order_number)              AS total_order,
  COUNT(DISTINCT customer_key)     AS number_of_customer,
  COUNT(DISTINCT product_key)      AS number_of_product
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- 4. Using FORMAT() — converts to string
SELECT
  FORMAT(order_date, 'MMMM-yyyy')  AS date,
  SUM(sales_amount)                AS total_sales,
  COUNT(order_number)              AS total_order,
  COUNT(DISTINCT customer_key)     AS number_of_customer,
  COUNT(DISTINCT product_key)      AS number_of_product
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'MMMM-yyyy')
ORDER BY FORMAT(order_date, 'MMMM-yyyy');

-- 5. Using DATETRUNC()
SELECT
  DATETRUNC(month, order_date)     AS date,
  SUM(sales_amount)                AS total_sales,
  COUNT(order_number)              AS total_order,
  COUNT(DISTINCT customer_key)     AS number_of_customer,
  COUNT(DISTINCT product_key)      AS number_of_product
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);

/*===============================================
CUMULATIVE ANALYSIS
===============================================*/
-- Aggregate values over time — track business growth/decline

-- 1. Running total of monthly sales
SELECT *,
  SUM(total_sales) OVER (ORDER BY order_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
FROM (
  SELECT
    DATETRUNC(month, order_date) AS order_month,
    SUM(sales_amount)            AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(month, order_date)
) AS t;

-- 1.1. Running total reset yearly
SELECT *,
  SUM(total_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
FROM (
  SELECT
    DATETRUNC(month, order_date) AS order_date,
    SUM(sales_amount)            AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(month, order_date)
) AS t;

/*===============================================
PERFORMANCE ANALYSIS
===============================================*/
-- Compare to target/average/previous — track performance trends

SELECT
  year_date,
  product_name,
  total_sales,
  AVG(total_sales) OVER (PARTITION BY product_name)                             AS avg_total_sales,
  total_sales - AVG(total_sales) OVER (PARTITION BY product_name)              AS diff_avg,
  CASE
    WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
    WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
    ELSE 'Avg'
  END                                                                            AS avg_change,
  LAG(total_sales,1) OVER (PARTITION BY product_name ORDER BY year_date)        AS previos_sales,
  total_sales - LAG(total_sales,1) OVER (PARTITION BY product_name ORDER BY year_date) AS diff_prev,
  CASE
    WHEN total_sales - LAG(total_sales,1) OVER (PARTITION BY product_name ORDER BY year_date) > 0 THEN 'Increasing'
    WHEN total_sales - LAG(total_sales,1) OVER (PARTITION BY product_name ORDER BY year_date) < 0 THEN 'Decreasing'
    ELSE 'Didnt Change'
  END                                                                            AS trend
FROM (
  SELECT
    DATETRUNC(year, order_date) AS year_date,
    dp.product_name,
    SUM(sales_amount)           AS total_sales
  FROM gold.fact_sales fs
  LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(year, order_date), dp.product_name
) AS t;

/*===============================================
PART TO WHOLE
===============================================*/
-- Show how each category contributes to total revenue

WITH cte_percentage AS (
  SELECT
    dp.category,
    SUM(fs.sales_amount) AS total_revenue_by_category
  FROM gold.fact_sales fs
  LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
  GROUP BY dp.category
)
SELECT
  category,
  total_revenue_by_category,
  SUM(total_revenue_by_category) OVER ()                                    AS total_all_sales,
  CAST(ROUND(CAST(total_revenue_by_category AS FLOAT) / SUM(total_revenue_by_category) OVER () * 100, 2) AS NVARCHAR) + '%' AS revenue_share
FROM cte_percentage
ORDER BY revenue_share DESC;

/*===============================================
DATA SEGMENTATION
===============================================*/
-- Divide products or customers based on defined value ranges

-- 1. Segment products by cost
WITH cte_segment AS (
  SELECT
    product_name,
    product_cost,
    CASE
      WHEN product_cost < 100 THEN 'Below 100'
      WHEN product_cost >= 100 AND product_cost < 500 THEN '100 - 500'
      WHEN product_cost > 500 AND product_cost <= 1000 THEN '501 - 1000'
      ELSE 'Above 1000'
    END AS segment_cost
  FROM gold.dim_products
)
SELECT *,
  COUNT(segment_cost) OVER (PARTITION BY segment_cost) AS count_segment
FROM cte_segment
ORDER BY count_segment DESC; -- You can also GROUP BY to summarize

-- 2. Segment customers by spend + lifetime
WITH cte_vipcust AS (
  SELECT
    fs.customer_key,
    ds.first_name,
    ds.last_name,
    CAST(MIN(fs.order_date) AS DATETIME) AS first_order,
    CAST(MAX(fs.order_date) AS DATETIME) AS last_order,
    SUM(fs.sales_amount)                AS spend
  FROM gold.fact_sales fs
  LEFT JOIN gold.dim_customers ds ON ds.customer_key = fs.customer_key
  GROUP BY fs.customer_key, ds.first_name, ds.last_name
)
SELECT
  segmentation_cust,
  COUNT(segmentation_cust) AS total_cust
FROM (
  SELECT *,
    CASE
      WHEN DATEDIFF(month, first_order, last_order) > 12 AND spend > 5000 THEN 'VIP'
      WHEN DATEDIFF(month, first_order, last_order) > 12 AND spend <= 5000 THEN 'Regular'
      ELSE 'New'
    END AS segmentation_cust
  FROM cte_vipcust
) AS t
GROUP BY segmentation_cust;

/*===============================================
CUSTOMER REPORT
===============================================*/
-- Full profile of customer activity and segmentation

WITH cte_report AS (
  SELECT
    f.order_number,
    f.customer_key,
    f.product_key,
    f.order_date,
    f.ship_date,
    f.due_date,
    f.sales_amount,
    f.quantitiy,
    f.price,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.marital_status,
    c.gender,
    DATEDIFF(year, c.birth_date, GETDATE()) AS customer_age,
    c.country,
    CASE
      WHEN DATEDIFF(year, c.birth_date, GETDATE()) < 18 THEN 'Under 18'
      WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 18 AND 30 THEN '18-30'
      WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 31 AND 60 THEN '31-60'
      ELSE 'Above 60'
    END AS age_category,
    CASE
      WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) > 12 AND SUM(sales_amount) > 5000 THEN 'VIP'
      WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) <= 12 AND SUM(sales_amount) <= 5000 THEN 'Reguler'
      ELSE 'New'
    END AS customer_categories
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
  WHERE order_date IS NOT NULL
  GROUP BY
    f.order_number, f.customer_key, f.product_key, f.order_date,
    f.ship_date, f.due_date, f.sales_amount, f.quantitiy, f.price,
    CONCAT(c.first_name, ' ', c.last_name), c.marital_status, c.gender,
    DATEDIFF(year, c.birth_date, GETDATE()), c.country
)
SELECT
  customer_key,
  customer_name,
  age_category,
  customer_categories,
  COUNT(DISTINCT order_number)         AS total_order,
  SUM(quantitiy)                       AS total_quantity_purchased,
  SUM(sales_amount)                   AS total_spend,
  COUNT(DISTINCT product_key)         AS total_product_purchased,
  DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
  DATEDIFF(month, MAX(order_date), GETDATE())       AS recency,
  AVG(quantitiy)                       AS avg_order_value,
  CASE
    WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) = 0 THEN CAST(SUM(sales_amount) AS NVARCHAR) + '$'
    ELSE CAST(ROUND(CAST(SUM(sales_amount) AS FLOAT) / DATEDIFF(month, MIN(order_date), MAX(order_date)), 2) AS NVARCHAR) + '$'
  END                                  AS avg_monthly_spend
FROM cte_report
GROUP BY customer_key, customer_name, age_category, customer_categories;

/*===============================================
PRODUCT REPORT
===============================================*/
-- Key metrics and segmentation for product performance

WITH cte_report_prod AS (
  SELECT
    s.order_number,
    s.product_key,
    s.customer_key,
    s.order_date,
    s.sales_amount,
    s.quantitiy,
    s.price,
    CASE
      WHEN s.price < 100 THEN 'Low-Performers'
      WHEN s.price BETWEEN 100 AND 1000 THEN 'Mid-Range'
      ELSE 'High-Performers'
    END AS revenue_segment,
    p.product_name,
    p.category,
    p.subcategory,
    p.product_cost
  FROM gold.fact_sales s
  LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
)
SELECT
  product_key,
  product_name,
  COUNT(order_number)                   AS total_order,
  SUM(quantitiy)                        AS total_quantity_sold,
  COUNT(DISTINCT customer_key)         AS total_customers,
  DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
  DATEDIFF(month, MAX(order_date), GETDATE())       AS recency,
  AVG(sales_amount)                    AS avg_revenue,
  CASE
    WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) = 0 THEN SUM(sales_amount)
    ELSE ROUND(CAST(SUM(sales_amount) AS FLOAT) / DATEDIFF(month, MIN(order_date), MAX(order_date)), 2)
  END                                  AS avg_monthly_revenue
FROM cte_report_prod
GROUP BY product_key, product_name;
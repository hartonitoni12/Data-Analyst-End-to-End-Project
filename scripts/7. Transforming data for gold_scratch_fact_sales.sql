use DataWarehouse

--1. selecting the data for the object

select
	sd.sls_ord_num as order_name,
	sd.sls_prd_key as product_key,
	sls_cust_id as customer_key,
	sls_order_dt as order_date,
	sls_ship_dt as ship_date,
	sls_due_dt as due_date,
	sls_sales as sales,
	sls_quantity as quantitiy,
	sls_price as price
from silver.crm_sales_details as sd

--2. join the fact table with the dimension table using surrogate key

select
	sd.sls_ord_num as order_number,
	ds.customer_key as customer_key,
	dp.product_key as product_key,
	sls_order_dt as order_date,
	sls_ship_dt as ship_date,
	sls_due_dt as due_date,
	sls_sales as sales,
	sls_quantity as quantitiy,
	sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products as dp
on sd.sls_prd_key = dp.product_number
left join gold.dim_customers as ds
on sd.sls_cust_id = ds.customer_id

-- 3. create views

create view gold.fact_sales
as
select
	sd.sls_ord_num as order_number,
	ds.customer_key as customer_key,
	dp.product_key as product_key,
	sls_order_dt as order_date,
	sls_ship_dt as ship_date,
	sls_due_dt as due_date,
	sls_sales as sales_amount,
	sls_quantity as quantitiy,
	sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products as dp
on sd.sls_prd_key = dp.product_number
left join gold.dim_customers as ds
on sd.sls_cust_id = ds.customer_id

-- 4. checking quality of views
--4.1 data integrity
select*
from gold.fact_sales as s
left join gold.dim_customers as c
on s.customer_key = c.customer_key
left join gold.dim_products as p
on s.product_key = p.product_key
where c.customer_key is null or p.product_key is null

--4.2 correctness of the data

select*
from gold.fact_sales

select*
from gold.dim_customers

select*
from gold.dim_products
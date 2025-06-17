use DataWarehouse

--1. selecting data for the object
select * from silver.crm_prd_info

select * from silver.erp_px_cat_g1v2

select
	p.prd_id as product_id,
	p.prd_cat_id as category_id,
	prd_key as product_key,
	c.cat as category,
	c.subcat as subcategory,
	p.prd_nm as product_name,
	p.prd_cost as product_cost,
	c.maintanance as maintanance,
	p.prd_start_dt as start_date,
	p.prd_end_dt as end_date
from silver.crm_prd_info as p
left join silver.erp_px_cat_g1v2 as c
on p.prd_cat_id = c.id

--2. clean the historical data

select
	p.prd_id as product_id,
	prd_key as product_key,
	p.prd_nm as product_name,
	p.prd_cat_id as category_id,
	c.cat as category,
	c.subcat as subcategory,
	p.prd_cost as product_cost,
	c.maintanance as maintanance,
	p.prd_start_dt as start_date
from silver.crm_prd_info as p
left join silver.erp_px_cat_g1v2 as c
on p.prd_cat_id = c.id
where p.prd_end_dt is null

--3. checking duplicate data

select product_key, count(*)
from(
select
	p.prd_id as product_id,
	prd_key as product_key,
	p.prd_nm as product_name,
	p.prd_cat_id as category_id,
	c.cat as category,
	c.subcat as subcategory,
	p.prd_cost as product_cost,
	c.maintanance as maintanance,
	p.prd_start_dt as start_date
from silver.crm_prd_info as p
left join silver.erp_px_cat_g1v2 as c
on p.prd_cat_id = c.id
where p.prd_end_dt is null
)t
group by product_key
having count(*) > 1 -- no duplicate data

--4. surrogate key

select
	rank() over (order by p.prd_start_dt,p.prd_key) as product_key,
	p.prd_id as product_id,
	prd_key as product_number,
	p.prd_nm as product_name,
	p.prd_cat_id as category_id,
	c.cat as category,
	c.subcat as subcategory,
	p.prd_cost as product_cost,
	c.maintanance as maintanance,
	p.prd_start_dt as start_date
from silver.crm_prd_info as p
left join silver.erp_px_cat_g1v2 as c
on p.prd_cat_id = c.id
where p.prd_end_dt is null

-- 5. create view

create view gold.dim_products as
select
	rank() over (order by p.prd_start_dt,p.prd_key) as product_key,
	p.prd_id as product_id,
	prd_key as product_number,
	p.prd_nm as product_name,
	p.prd_cat_id as category_id,
	c.cat as category,
	c.subcat as subcategory,
	p.prd_cost as product_cost,
	c.maintanance as maintanance,
	p.prd_start_dt as start_date
from silver.crm_prd_info as p
left join silver.erp_px_cat_g1v2 as c
on p.prd_cat_id = c.id
where p.prd_end_dt is null


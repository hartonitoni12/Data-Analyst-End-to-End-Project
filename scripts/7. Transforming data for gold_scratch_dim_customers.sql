use DataWarehouse

--1. selecting data for gold layer
select
	c.cst_id as customer_id,
	c.cst_key as customer_key,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	c.cst_gndr as gender,
	a.bdate as birth_date,
	a.gen as gender_2,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid

--2. checking duplicate data

select customer_key, count(*) as numbrdt
from(select
	c.cst_id as customer_id,
	c.cst_key as customer_key,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	c.cst_gndr as gender,
	a.bdate as birth_date,
	a.gen as gender_2,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid)t
group by customer_key
having count(*) > 1 --no duplicates data

--3. data integration of gender

select distinct gender,gender_2
from(select
	c.cst_id as customer_id,
	c.cst_key as customer_key,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	c.cst_gndr as gender,
	a.bdate as birth_date,
	a.gen as gender_2,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid)t

/* rules :
	- The crm_cust_info is the master table, so the reference is based on it, when there is unnmatching data betwen gender on the join table,
	  we use gender from master table.
	- if the master table gender data is not avalible, then ref the gender from erp_cust_az12
*/

select distinct gender,gender_2,
	case
		when gender <> 'n/a' then gender
		when gender = 'n/a' and gender_2 is null then 'n/a'
		else gender_2
	end as new_gen
from(select
	c.cst_id as customer_id,
	c.cst_key as customer_key,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	c.cst_gndr as gender,
	a.bdate as birth_date,
	a.gen as gender_2,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid)t

--3.1 change the main query

select
	c.cst_id as customer_id,
	c.cst_key as customer_key,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	case
		when c.cst_gndr <> 'n/a' then c.cst_gndr
		when c.cst_gndr = 'n/a' and a.gen is null then 'n/a'
		else a.gen
	end as gender,
	a.bdate as birth_date,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid

--4. surrogate key

select
	rank() over (order by cst_id) as customer_key,
	c.cst_id as customer_id,
	c.cst_key as customer_number,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	case
		when c.cst_gndr <> 'n/a' then c.cst_gndr
		when c.cst_gndr = 'n/a' and a.gen is null then 'n/a'
		else a.gen
	end as gender,
	a.bdate as birth_date,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid

--5. create view

create view gold.dim_customers as
select
	rank() over (order by cst_id) as customer_key,
	c.cst_id as customer_id,
	c.cst_key as customer_number,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	c.cst_marital_status as marital_status,
	case
		when c.cst_gndr <> 'n/a' then c.cst_gndr
		when c.cst_gndr = 'n/a' and a.gen is null then 'n/a'
		else a.gen
	end as gender,
	a.bdate as birth_date,
	l.cntry as country,
	c.cst_create_date as create_date
from silver.crm_cust_info as c
left join silver.erp_cust_az12 as a
on c.cst_key = a.cid
left join silver.erp_loc_a101 as l
on c.cst_key = l.cid

--6. check quality of the object

--6.1 make sure all the data placed in correct column

select * from gold.dim_customers

--6.2 check the gender column

select distinct gender from gold.dim_customers
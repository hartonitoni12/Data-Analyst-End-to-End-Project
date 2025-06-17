use datawarehouse

/* 
===========================================Cleaning and Transforming CRM cust info Table=====================================
*/
select * from bronze.crm_cust_info

--1. checking duplicate and null primary key
select cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 -- We have some null and duplivate primary key

-- 1.1 clean the duplicate pk
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
from(select *,
rank() over (partition by cst_id order by cst_create_date desc) as rank
from bronze.crm_cust_info
where cst_id is not null)s
where rank = 1

--2. check for unwanted spaces on cst_firstname, cst_lastname, and cst_key
select cst_firstname
from bronze.crm_cust_info
where trim(cst_firstname) <> cst_firstname -- we have lots of unwanted spaces

select cst_lastname
from bronze.crm_cust_info
where trim(cst_lastname) <> cst_lastname -- we have lots of unwanted spaces

select cst_key
from bronze.crm_cust_info
where trim(cst_key) <> cst_key

-- 2.1 Clean the unwanted spaces
	
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
from(select *,
rank() over (partition by cst_id order by cst_create_date desc) as rank
from bronze.crm_cust_info
where cst_id is not null)s
where rank = 1

-- 3. check the standardization and consistency on cst_gndr and cst_marital_status

select distinct cst_gndr
from bronze.crm_cust_info -- There are null values and the values is not user friendly

select distinct cst_marital_status
from bronze.crm_cust_info -- There are null values and the values is not user friendly

-- 3.1 Change f and M into female and male and s and M into single and married

select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname, --Data Cleaning
	TRIM(cst_lastname) as cst_lastname, --Data Cleaning
	CASE
		when cst_marital_status = 'S' then 'Single'
		when cst_marital_status = 'M' then 'Married'
		else 'n/a'
	end as cst_marital_status, --Data Normalization and Standardization
	CASE
		when cst_gndr = 'F' then 'Female'
		when cst_gndr = 'M' then 'Male'
		else 'n/a'
	end as cst_gndr,--Data Normalization and Standardization
	cst_create_date
from(select *,
rank() over (partition by cst_id order by cst_create_date desc) as rank
from bronze.crm_cust_info
where cst_id is not null)s --Handling duplicate data and null values
where rank = 1

-- 4. insert into silver.crm_cust_info
insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname, --Data Cleaning
	TRIM(cst_lastname) as cst_lastname, --Data Cleaning
	CASE
		when cst_marital_status = 'S' then 'Single'
		when cst_marital_status = 'M' then 'Married'
		else 'n/a'
	end as cst_marital_status, --Data Normalization and Standardization
	CASE
		when cst_gndr = 'F' then 'Female'
		when cst_gndr = 'M' then 'Male'
		else 'n/a'
	end as cst_gndr,--Data Normalization and Standardization
	cst_create_date
from(select *,
rank() over (partition by cst_id order by cst_create_date desc) as rank
from bronze.crm_cust_info
where cst_id is not null)s --Handling duplicate data and null values
where rank = 1

-- 5. Quality check of silver table
-- checking duplicate and null pk
select cst_id,
	count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1

-- check for unwanted spaces
select cst_firstname
from silver.crm_cust_info
where trim(cst_firstname) <> cst_firstname

select cst_lastname
from silver.crm_cust_info
where trim(cst_lastname) <> cst_lastname

select cst_key
from silver.crm_cust_info
where trim(cst_key) <> cst_key

-- check data standardization and consistency
select distinct cst_gndr
from silver.crm_cust_info

select distinct cst_marital_status
from silver.crm_cust_info

-- check all the data
select *
from silver.crm_cust_info

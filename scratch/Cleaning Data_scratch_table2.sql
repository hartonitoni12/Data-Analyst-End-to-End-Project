use DataWarehouse
/*
==========================================Cleaning The bronze.crm_prd_info Table===========================================
*/
select top 1000 *
from bronze.crm_prd_info

select top 1000 *
from bronze.crm_sales_details

select top 1000 *
from bronze.erp_px_cat_g1v2

select
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

-- * As we can see, the prd_key is not actually the original key, its a combinaton of category id and prd_key itself, wee need to clean it

-- 1. Checking duplicates and null data in pk

select prd_id,
count(*) as counttotal
from bronze.crm_prd_info
group by prd_id
having count(*)>1 -- No duplicate

select prd_id
from bronze.crm_prd_info
where prd_id is null -- No null data

--2. Substract the category product

select prd_key,
	substring(prd_key,1,5) as cat,
	substring(prd_key,7,len(prd_key)) as prd
from bronze.crm_prd_info

-- 2.1 add cateory and product info

select
	prd_id,
	prd_key,
	replace(substring(prd_key,1,5),'-','_') as prd_cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key_name,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

-- 3. Checking unwanted spaces in prd_name

select prd_nm
from silver.crm_prd_info
where trim(prd_nm) <> prd_nm -- we dont have any unwanted spaces in prd_name

-- 4. Checking data standadization and data consistency

select distinct prd_line
from bronze.crm_prd_info -- Not user friendly, there are null

-- 4.1 change M R S T into user friendly

select
	prd_id,
	prd_key,
	replace(substring(prd_key,1,5),'-','_') as prd_cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key_name,
	CASE
		when prd_line = 'M' then 'Mountain'
		when prd_line = 'R' then 'Road'
		when prd_line = 'T' then 'Touring'
		when prd_line = 'S' then 'Other Sales'
		else 'n/a'
	end as prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

-- 5. Check the correctness of prd_cost
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null --There are Null Data

-- 5.1 change the uncorrect prd_cost
select
	prd_id,
	prd_key,
	replace(substring(prd_key,1,5),'-','_') as prd_cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key_name,
	CASE
		when prd_line = 'M' then 'Mountain'
		when prd_line = 'R' then 'Road'
		when prd_line = 'T' then 'Touring'
		when prd_line = 'S' then 'Other Sales'
		else 'n/a'
	end as prd_nm,
	isnull(prd_cost,0) as prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

-- 6. Checking the correctness of date data
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt --Lots of data that have an error date data

-- 6.1 Clean Error Date Data
select *,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as end_dt
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- 6.2 transforming to new prd_end_dt
select
	prd_id,
	prd_key,
	replace(substring(prd_key,1,5),'-','_') as prd_cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key_name,
	CASE
		when prd_line = 'M' then 'Mountain'
		when prd_line = 'R' then 'Road'
		when prd_line = 'T' then 'Touring'
		when prd_line = 'S' then 'Other Sales'
		else 'n/a'
	end as prd_nm,
	isnull(prd_cost,0) as prd_cost,
	prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast((lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1) as date) as prd_end_dt
from bronze.crm_prd_info

-- * Because we've changed the datatype of the column, we need to redifined the table datatype column

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
    prd_id      INT,
	prd_cat_id	NVARCHAR(50),
    prd_key     NVARCHAR(50),
    prd_nm      NVARCHAR(50),
    prd_cost    INT,
    prd_line    NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dhw_create_data		datetime2 default getdate()
);
GO

-- 7. Insert Data into Table

insert into silver.crm_prd_info(
	prd_id,
	prd_cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select
	prd_id,
	replace(substring(prd_key,1,5),'-','_') as prd_cat_id, --deriving data => data enrichment
	substring(prd_key,7,len(prd_key)) as prd_key, --deriving data => data enrichment
	prd_nm,
	isnull(prd_cost,0) as prd_cost, --handling null data
	CASE
		when trim(prd_line) = 'M' then 'Mountain'
		when trim(prd_line) = 'R' then 'Road'
		when trim(prd_line) = 'T' then 'Touring'
		when trim(prd_line) = 'S' then 'Other Sales'
		else 'n/a'
	end as prd_line, --data normalization
	cast(prd_start_dt as date) as prd_start_dt, --data transformation
	cast((lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1) as date) as prd_end_dt --data enrichment
from bronze.crm_prd_info

-- 8. checking quality of the table data
select * from silver.crm_prd_info
where prd_cost is null

-- checking duplicates and null data in pk
select prd_id,
count(*) as counttotal
from silver.crm_prd_info
group by prd_id
having count(*)>1
-- checking unwanted spaces
select prd_nm
from silver.crm_prd_info
where trim(prd_nm) <> prd_nm
-- checking data standadization and data consistency
select distinct prd_line
from silver.crm_prd_info
-- check the correctness of product cost
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null
-- checking the correctness of date data
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt
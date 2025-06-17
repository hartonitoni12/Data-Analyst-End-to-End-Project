use DataWarehouse

/*
=======================================Cleaning bronze.erp_loc_a101=================================================
*/

select
	cid,
	cntry
from bronze.erp_loc_a101

-- 1. checking duplicate pk

select cid,
count (*)
from bronze.erp_loc_a101
group by cid
having count (*) > 1 --All Good

-- 2. checking cid integrity with cid cust_az12

select cid
from bronze.erp_loc_a101

select cid
from silver.erp_cust_az12

select c.cid, z.cid
from bronze.erp_loc_a101 as c
left join silver.erp_cust_az12 as z
on c.cid = z.cid
where z.cid is null --so cid in this table have extra '-' characteras seperator

--2.1 transforming the invalid data
select cid,
	replace(cid,'-','') as new_cid
from bronze.erp_loc_a101

-- recheck integrity
select s.new_cid,c.cst_key
from(select cid,
	replace(cid,'-','') as new_cid
from bronze.erp_loc_a101) as s
left join silver.crm_cust_info as c
on s.new_cid = c.cst_key
where c.cst_key is null -- basically we have 1 more data in this table

--2.2 include the change into the applied query
select
	replace(cid,'-','') as cid,
	cntry
from bronze.erp_loc_a101

-- 3 check the data quality of the country
select distinct cntry
from bronze.erp_loc_a101 --Bad quality of data, not standardized yet

--3.1 standardize the cntry data

select distinct
	case
		when trim(cntry) in ('US','USA','United States') then 'United States'
		when trim(cntry) in ('DE','Germany') then 'Germany'
		when trim(cntry) = '' or trim(cntry) is null then 'n/a'
		else trim(cntry)
	end as cntry
from bronze.erp_loc_a101

--3.2 include the change  into the query
select
	replace(cid,'-','') as cid, -- handling invalid data
	case
		when trim(cntry) in ('US','USA','United States') then 'United States' --standardize data, handling missing data
		when trim(cntry) in ('DE','Germany') then 'Germany'
		when trim(cntry) = '' or trim(cntry) is null then 'n/a'
		else trim(cntry)
	end as cntry
from bronze.erp_loc_a101

-- 4. Load the data into silver layer
insert into silver.erp_loc_a101(
	cid,
	cntry
)
select
	replace(cid,'-','') as cid,
	case
		when trim(cntry) in ('US','USA','United States') then 'United States'
		when trim(cntry) in ('DE','Germany') then 'Germany'
		when trim(cntry) = '' or trim(cntry) is null then 'n/a'
		else trim(cntry)
	end as cntry
from bronze.erp_loc_a101
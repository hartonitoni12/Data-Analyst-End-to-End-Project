use DataWarehouse

/*
=====================================================Cleaning Bronze.erp_cust_az12============================================
*/

select
	cid,
	bdate,
	gen
from bronze.erp_cust_az12

select *
from bronze.crm_cust_info

--1. check the double pk

select cid,
count(*)
from bronze.erp_cust_az12
group by cid
having count(*) > 1 -- All good

-- 2. check the integrity of cst id

select cid, cst_key
from bronze.erp_cust_az12 as e
full join bronze.crm_cust_info as c
on e.cid = c.cst_key
where cid is null or cst_key is null

select cid, cst_key
from bronze.erp_cust_az12 as e
inner join bronze.crm_cust_info as c
on e.cid = c.cst_key
-- So, there are some cid that have extra characters, and some of them are not

--2.1 clean the data with extra character in cid

select cid,
case
	when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end as cid_new
from bronze.erp_cust_az12

-- 2.2 input in the query
select
	case
		when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end as cid,
	bdate,
	gen
from bronze.erp_cust_az12

--3. quality of bdate data
select bdate
from bronze.erp_cust_az12
where bdate >= getdate() or bdate <= '1925-01-01' -- There are some birthdate that are not rationally correct

--3.1 clean the invalid data
select bdate,
case
	when bdate >= getdate() or bdate <= '1925-01-01' then null
	else bdate
end as new_bdate
from bronze.erp_cust_az12

--check
select *
from(select bdate,
case
	when bdate >= getdate() or bdate <= '1925-01-01' then null
	else bdate
end as new_bdate
from bronze.erp_cust_az12)s
where  new_bdate >= getdate() or new_bdate <= '1925-01-01'

--3.2 insert into query

select
	case
		when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end as cid,
	case
		when bdate >= getdate() then null
		else bdate
	end as bdate,
	gen
from bronze.erp_cust_az12

-- 4. check the quality of gen data
select distinct trim(gen)
from bronze.erp_cust_az12 -- Not Standardized

-- 4.1 transforming the bad quality data of gen

select gen,
	case
		when trim(gen) = 'F' then 'Female' 
		when trim(gen) = 'M' then 'Male' 
		when trim(gen) = '' or trim(gen) is null then 'n/a'
		else trim(gen)
	end as new_gen
from bronze.erp_cust_az12

-- recheck
select distinct new_gen
from(select gen,
	case
		when trim(gen) = 'F' then 'Female' 
		when trim(gen) = 'M' then 'Male' 
		when trim(gen) = '' or trim(gen) is null then 'n/a'
		else trim(gen)
	end as new_gen
from bronze.erp_cust_az12)s --its good

-- 4.2 input into the load query

select
	case
		when cid like 'NAS%' then substring(cid, 4, len(cid)) --  handle invalid values
		else cid
	end as cid,
	case
		when bdate >= getdate() then null -- handle invalid values
		else bdate
	end as bdate,
	case
		when trim(gen) = 'F' then 'Female' 
		when trim(gen) = 'M' then 'Male' 
		when trim(gen) = '' or trim(gen) is null then 'n/a' -- data standardization
		else trim(gen)
	end as gen
from bronze.erp_cust_az12

-- 5. load into silver layer

insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
select
	case
		when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end as cid,
	case
		when bdate >= getdate() then null
		else bdate
	end as bdate,
	case
		when trim(gen) = 'F' then 'Female' 
		when trim(gen) = 'M' then 'Male' 
		when trim(gen) = '' or trim(gen) is null then 'n/a'
		else trim(gen)
	end as gen
from bronze.erp_cust_az12

-- 6. recheck loaded data
-- check the duplicate pk
select cid,
count(*)
from silver.erp_cust_az12
group by cid
having count(*) > 1

-- check the integrity of cid and cst_key
select cid, cst_key
from silver.erp_cust_az12 as e
left join bronze.crm_cust_info as c
on e.cid = c.cst_key
where cid is null

-- check quality of bdate data
select bdate
from silver.erp_cust_az12
where bdate >= getdate() 

-- check the quality and stardadization of gen data
select distinct trim(gen)
from silver.erp_cust_az12
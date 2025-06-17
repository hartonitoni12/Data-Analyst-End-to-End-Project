use DataWarehouse

/*
=======================================================Cleaning bronze.erp_px_cat_g1v2===================================================
*/

select 
	id,
	cat,
	subcat,
	maintanance
from bronze.erp_px_cat_g1v2

-- 1. check duplicates pk

select id,
	count(*)
from bronze.erp_px_cat_g1v2
group by id
having count(*) > 1 -- good

-- 1.1 check unwanted space

select id
from bronze.erp_px_cat_g1v2
where trim(id) <> id --good

-- 2. CHecking the consistency and quality of cat data

select distinct cat
from bronze.erp_px_cat_g1v2 -- good

-- 3. checking the consstency of subcat data

select distinct subcat
from bronze.erp_px_cat_g1v2 --good

--4. checking the consistency of maintanance data

select distinct maintanance
from bronze.erp_px_cat_g1v2 -- good

--5. checking data integration between prd_info and this table

select b.id, s.prd_cat_id
from bronze.erp_px_cat_g1v2 as b
left join silver.crm_prd_info as s
on b.id = s.prd_cat_id
where s.prd_cat_id is null -- there is one category is never been sale before

-- 6. load the data to siler layer

insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintanance
)
select 
	id,
	cat,
	subcat,
	maintanance
from bronze.erp_px_cat_g1v2

-- checking quality of loaded data
-- check unwanted space

select id
from silver.erp_px_cat_g1v2
where trim(id) <> id

-- checking the consistency and quality of cat data

select distinct cat
from silver.erp_px_cat_g1v2 -- good

--checking the consstency of subcat data

select distinct subcat
from silver.erp_px_cat_g1v2 --good

--checking the consistency of maintanance data

select distinct maintanance
from silver.erp_px_cat_g1v2

-- check all loaded data
select *
from silver.erp_px_cat_g1v2
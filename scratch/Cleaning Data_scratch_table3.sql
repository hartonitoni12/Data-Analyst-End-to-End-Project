use DataWarehouse

/*
===============================================Cleaning Bronze.crm_sales_details===================================================
*/

select *
from bronze.crm_sales_details

-- 1. Checking duplicate pk

select sls_ord_num,
count(*)
from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1 -- We Have A LOTS

-- 1.1 inspect the deep attribute of the duplicate pk
select *
from bronze.crm_sales_details
where sls_ord_num in(
	select sls_ord_num
	from
	(select sls_ord_num,
	count(*) over (partition by sls_ord_num) as count
	from bronze.crm_sales_details)t
	where count > 1) -- Its duplicate because there are customers that order more than 1 product, so we keep it that way

-- 2. Inspect The correctness of Date Data

-- 2.1 checking the sls order dt quality
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) <> 8
or sls_order_dt >= 20500101
or sls_order_dt <= 19000101 --We have some errors

-- 2.2 checking the sls_ship_dt quality
select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or len(sls_ship_dt) <> 8
or sls_ship_dt >= 20500101
or sls_ship_dt <= 19000101 --No errors

-- 2.3 checking the sls_due_dt
select sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
or len(sls_due_dt) <> 8
or sls_due_dt >= 20500101
or sls_due_dt <= 19000101 --No errors

-- 2.4 checking correctness of order date and ship date
select*
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt --No errors


-- 2.5 Handle the 0 value and transform the dt from int to date datatype (len <> 8)
select*
from(select sls_order_dt,
	case
		when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null
		else cast(cast(sls_order_dt as nvarchar) as date)
	end as sls_order_dt1
from bronze.crm_sales_details)s
where sls_order_dt1 <= 0 or len(sls_order_dt1) <> 8

select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case
		when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null
		else cast(cast(sls_order_dt as nvarchar) as date)
	end as sls_order_dt,
	case
		when sls_ship_dt = 0 or len(sls_ship_dt) <> 8 then null
		else cast(cast(sls_ship_dt as nvarchar) as date)
	end as sls_ship_dt,
	case
		when sls_due_dt = 0 or len(sls_due_dt) <> 8 then null
		else cast(cast(sls_due_dt as nvarchar) as date)
	end as sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details

--3. check the data quality of the sales, quant, and price
--3.1 checking the negative value, null value, and uncorrect value

select *
from bronze.crm_sales_details
where sls_sales <=0 or sls_quantity <= 0 or sls_price <= 0 
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <> sls_price*sls_quantity

--3.2 transforming data
-- if the sales negative, 0, null, or unmatch with price*quantity, then transform based on price and quantitiy value
-- if the price zero and null, match the value based on saes and quantitiy
-- if price is negative, transform it into positive

select *
from(
select
case
	when sls_sales <= 0 or sls_sales is null or sls_sales <> sls_price*sls_quantity then abs(sls_price*sls_quantity)
	else sls_sales
end as sales,
sls_quantity,
case
	when sls_price = 0 or sls_price is null then abs(sls_sales/nullif(sls_quantity,0))
	when sls_price < 0 then abs(sls_price)
	else sls_price
end as price
from bronze.crm_sales_details)s
where sales <= 0 or sales is null or price is null or price <= 0
or sales <> price*sls_quantity

-- We got the final query

select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case
		when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null
		else cast(cast(sls_order_dt as nvarchar) as date)
	end as sls_order_dt,
	case
		when sls_ship_dt = 0 or len(sls_ship_dt) <> 8 then null
		else cast(cast(sls_ship_dt as nvarchar) as date)
	end as sls_ship_dt,
	case
		when sls_due_dt = 0 or len(sls_due_dt) <> 8 then null
		else cast(cast(sls_due_dt as nvarchar) as date)
	end as sls_due_dt,
	case
		when sls_sales <= 0 or sls_sales is null or sls_sales <> sls_price*sls_quantity then abs(sls_price*sls_quantity)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case
		when sls_price = 0 or sls_price is null then abs(sls_sales/nullif(sls_quantity,0))
		when sls_price < 0 then abs(sls_price)
		else sls_price
	end as sls_price
from bronze.crm_sales_details

-- 4. inserting data into silver layer
-- change the ddl
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt date,
    sls_ship_dt  date,
    sls_due_dt   date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dhw_create_data	datetime2 default getdate()
);
GO

insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case
		when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null -- handling invalid data
		else cast(cast(sls_order_dt as nvarchar) as date) -- changing datatype
	end as sls_order_dt,
	case
		when sls_ship_dt = 0 or len(sls_ship_dt) <> 8 then null
		else cast(cast(sls_ship_dt as nvarchar) as date)
	end as sls_ship_dt,
	case
		when sls_due_dt = 0 or len(sls_due_dt) <> 8 then null
		else cast(cast(sls_due_dt as nvarchar) as date)
	end as sls_due_dt,
	case
		when sls_sales <= 0 or sls_sales is null or sls_sales <> sls_price*sls_quantity then abs(sls_price*sls_quantity) -- handling invalid and missing data
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case
		when sls_price = 0 or sls_price is null then abs(sls_sales/nullif(sls_quantity,0)) -- handling invalid and missing data
		when sls_price < 0 then abs(sls_price)
		else sls_price
	end as sls_price
from bronze.crm_sales_details

--5. checking quality of loaded data
-- checking the correctness of the date data
select*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--checking the qaulity if sales, price and quantity data
select*
from silver.crm_sales_details
where sls_sales <= 0 or sls_sales is null
or sls_price <= 0 or sls_price is null
or sls_quantity <= 0 or sls_quantity is null
or sls_sales <> sls_price*sls_quantity

-- check the correctness of the loaded data
select *
from silver.crm_sales_details

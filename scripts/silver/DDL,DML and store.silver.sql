----Building the silver layer 
---- IN all the column I have added one extra column which dwh_create_date which give the database creation timmings in datetime
----format .

if OBJECT_ID ('silver.crm_cust_info','u') is not null
   drop table silver.crm_cust_info
-- customer information table 
create table silver.crm_cust_info(
cst_id int , 
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.prd_info','u') is not null
   drop table silver.prd_info
--production information table 
create table silver.prd_info(
prd_id int ,
prd_key nvarchar(50),
prd_nm nvarchar(50),        
prd_cost int ,
prd_line    nvarchar(50),
prd_start_dt  datetime,
prd_end_dt datetime,
dwh_create_date datetime2 default getdate());


if OBJECT_ID ('silver.crm_sales_details','u') is not null
   drop table silver.crm_sales_details
--sales information table 
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate());


if OBJECT_ID ('silver.erp_loc_a101','u') is not null
   drop table silver.erp_loc_a101
---create enterprise resource planning tables 
create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate());


if OBJECT_ID ('silver.erp_cust_az12','u') is not null
   drop table silver.erp_cust_az12
create table silver.erp_cust_az12(
cid  nvarchar(50),
bdate date ,
gen nvarchar(50),
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.erp_px_cat_g1v2','u') is not null
   drop table silver.erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate());

--checking for the nulls and the  duplicates in the  primary key 
select 
       cst_id,
       count(*) as [number of customers]
from bronze.crm_cust_info
group by cst_id
having count(*) > 1  or  cst_id is null

--we have five customer id which are duplicates in the table 
-- and three rows are blank 
-- will use window function row_number here to assign a unique value to each row in the  result for some  
--- based on a defined order 

select 
*
from(
select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc ) as flag_last
from bronze.crm_cust_info) t
where flag_last ! =1

--- check for unwanted spaces 
select 
      cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


select 
      cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

-- THIS IS FOR THE CUSTOMER INFORMATION TABLE 
--checking trimmed values of the table without null and duplicates and replacing  it will tranformed values in the silver schema 
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date )

select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case 
        when upper(trim(cst_gndr))='F' then 'Female'
        when upper(trim(cst_gndr))='M' then 'Male'
        else 'NA'
        end as cst_gndr ,
 case 
        when upper(trim(cst_marital_status))='S' then 'Single'
        when upper(trim(cst_marital_status))='M' then 'Married'
        else 'NA'            
        end as cst_marital_status,
            cst_create_date

from(select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc ) as flag_last
from bronze.crm_cust_info
where cst_id is not null)t
where flag_last = 1
 
select * from bronze.crm_cust_info

------changing the column name from cst_material_status to cst_marital_status in both schemas 
EXEC sp_rename 'bronze.crm_cust_info.cst_material_status', 'cst_marital_status', 'column';
EXEC sp_rename 'silver.crm_cust_info.cst_material_status', 'cst_marital_status', 'column';

---- data standardization and consistency
select 
    distinct cst_marital_status 
from bronze.crm_cust_info

---check for unwanted spaces
Select cst_lastname                                                                                            
from silver.crm_cust_info                                                                                                            
where  cst_lastname != trim(cst_lastname)                                                                           

select * from silver.crm_cust_info;

-- THIS IS FOR THE PRODUCT INFORMATION TABLE 
select * from bronze.prd_info
SELECT 
        prd_id,
        count(*) as total_count
from bronze.prd_info
group by prd_id
having  count(*) > 1 or prd_id is null



select 
prd_id,
prd_key,
replace (substring(prd_key , 1, 5) ,'-','_') as cat_id,
substring(prd_key , 7, len(prd_key)) as prd_key ,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.prd_info
where replace (substring(prd_key ,1, 5) ,'-','_')  not in (
select distinct id from bronze.erp_px_cat_g1v2 )



ALTER TABLE silver.prd_info
ADD cat_id varchar(50)


insert into silver.prd_info
(prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)

select 
prd_id,
replace(substring(prd_key ,1,5),'-','_') as cat_id,
substring(prd_key ,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case 
           when upper(trim(prd_line))='M' then 'Mountain'
             when upper(trim(prd_line))='R' then 'Road'
            when upper(trim(prd_line))='S' then 'Other Sales'
           when upper(trim(prd_line))='T' then 'Touring'
 else 'NA'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.prd_info

select * from silver.prd_info;
--- check for invalid date orders
select *
from bronze.prd_info
where prd_end_dt < prd_start_dt

----------- FOR THE SALES INFORMATION

insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_due_dt,
sls_ship_dt,
sls_sales,
sls_quantity,
sls_price)

select 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      case 
            when sls_order_dt=0 or len(sls_order_dt) !=8 then null
            else cast(cast(sls_order_dt as varchar) as date)
            end as sls_order_dt,

       case
            when  sls_due_dt=0 or len(sls_due_dt) !=8 then null
            else cast(cast(sls_due_dt as varchar) as date) 
            end as sls_due_dt,


      case 
            when sls_ship_dt=0 or len(sls_ship_dt) != 8 then null
            else cast(cast(sls_ship_dt as varchar) as date) 
            end as sls_ship_dt,
      
      case 
            when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
            then sls_quantity * abs(sls_price)
            else sls_sales 
            end as  sls_sales ,
sls_quantity,

      case 
            when sls_price is null or sls_price <=0 then sls_sales/ nullif(sls_quantity , 0)
            else sls_price  
            end as sls_price 
from bronze.crm_sales_details;  ------- error in this code what's wrong here 

select *  from bronze.crm_sales_details

--checking the datatypes of the columns 
sp_help 'bronze.crm_sales_details'

---- FOR THE erp_cust_az12 table 
insert into silver.erp_cust_az12 (
cid,
bdate,
gen)

select 
      case  
           when cid like 'NAS%' Then substring(cid , 4 , len(cid)) 
           else cid 
           end as cid,
      case
           when bdate> getdate() then null 
           else bdate
           end as bdate,


      case
          when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
          when upper(trim(gen)) in ('M', 'Male') then 'Male'
          else 'NA'
     end as gen 
from bronze.erp_cust_az12


---FOR THE erp_loc_a101 

insert into silver.erp_loc_a101(
cid,
cntry)

select 
      replace(cid,'-','') as cid,

      case
          when TRIM(cntry)='DE' then 'Germany'
          When trim(cntry) in ('us','usa') then 'United States'
          when trim(cntry) = '' or cntry is null then 'NA'
          else trim(cntry)
        end as cntry
 from bronze.erp_loc_a101[
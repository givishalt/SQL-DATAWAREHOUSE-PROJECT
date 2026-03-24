use datawarehouse;
--checking whether the table pre existed if yes then drop for each tables.
if OBJECT_ID ('bronze.crm_cust_info','u') is not null
   drop table bronze.crm_cust_info
-- customer information table 
create table bronze.crm_cust_info(
cst_id int , 
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date);

if OBJECT_ID ('bronze.prd_info','u') is not null
   drop table bronze.prd_info
--production information table 
create table bronze.prd_info(
prd_id int ,
prd_key nvarchar(50),
prd_nm nvarchar(50),        
prd_cost int ,
prd_line    nvarchar(50),
prd_start_dt  datetime,
prd_end_dt datetime);


if OBJECT_ID ('bronze.crm_sales_details','u') is not null
   drop table bronze.crm_sales_details
--sales information table 
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int);


if OBJECT_ID ('bronze.erp_loc_a101','u') is not null
   drop table bronze.erp_loc_a101
---create enterprise resource planning tables 
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50));


if OBJECT_ID ('bronze.erp_cust_az12','u') is not null
   drop table bronze.erp_cust_az12
create table bronze.erp_cust_az12(
cid  nvarchar(50),
bdate date ,
gen nvarchar(50));

if OBJECT_ID ('bronze.erp_px_cat_g1v2','u') is not null
   drop table bronze.erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50));


--insert values in the table 
bulk insert bronze.crm_cust_info
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (firstrow=2 ,
      fieldterminator=',',
      tablock)

--check inserted values in the table
select * from bronze.crm_cust_info


bulk insert bronze.prd_info
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with ( firstrow=2,
      fieldterminator=',',
      tablock)

--check inserted values in the table
select * from bronze.prd_info

bulk insert bronze.crm_sales_details
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with ( firstrow=2,
fieldterminator=',',
tablock);

--check inserted values in the table
select * from bronze.crm_sales_details

bulk insert bronze.erp_loc_a101
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with ( firstrow= 2,
fieldterminator=',',
tablock);

--check inserted values in the table
select * from bronze.erp_loc_a101

bulk insert bronze.erp_cust_az12
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with ( firstrow=2,
 fieldterminator=',',
 tablock);

--check inserted values in the table
select * from bronze.erp_cust_az12


bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\lenovo\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with ( firstrow=2,
fieldterminator=',',
tablock)

--check inserted values in the table
select * from bronze.erp_px_cat_g1v2

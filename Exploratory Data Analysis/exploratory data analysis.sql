############################################################################################################################

DATABASE CREATION

############################################################################################################################
create database data_analystics;
use  data_analystics;

--renaming according to the project 
alter database data_analystics
modify name = datawarehouse_data_analytics

--setting the single user access to multiusers
alter database datawarehouse
set multi_user;
use datawarehouse;

select * from gold.dim_customers
select * from gold.dim_products
select * from gold.fact_sales


use datawarehouse_data_analytics

----importing all the required data via flat files 

select * from dim_customers
go
select * from dim_products
go
select * from fact_sales

########################################################################################################
DATABASE EXPLORATION 
########################################################################################################

Purpose :>>> To explore the structure of  the database including the list of tables and their
schemas.
>>> To inspect the columns and metadata for specific tables 
########################################################################################################

--- Explore all objects in the data 
select * from INFORMATION_SCHEMA.tables

---- explore all the columns in the database
select * from INFORMATION_SCHEMA.columns
where  TABLE_NAME= 'dim_customers'

###########################################################################################################################

Dimension exploration

############################################################################################################################

SQL function used :
--distinct
--order by

############################################################################################################################
list of unique countries from which customers originate 
select 
       distinct country 
from dim_customers
order by country ;

#############################################################################################################################

-- list of unique categories , subcategories , and products

select distinct 
       category,
       subcategory,
       product_name
from dim_products
order by category, subcategory , product_name;

################################################################################################################

Date Range exploration

################################################################################################################

--Determine the first and last order date and the total duration in months
select * from dim_customers;
select * from dim_products
select * from fact_sales;
select 
      MIN(start_date) as first_order_date,
      MAX(order_date) as last_order_date,
      DATEDIFF(month , min(order_date), max(order_date)) as order_range_months

from fact_sales;

#####################################################################################################################

---Find the youngest and oldest customer based on birthdate
select 
      MIN(birthdate)  as oldest_customers ,
      DATEDIFF(YEAR , MIN(birthdate) , getdate()) as oldest_age,
      MAX(birthdate) as youngest_customers,
      DATEDIFF( year , max(birthdate) , getdate()) as youngest_age
from dim_customers;


#######################################################################################################################

purpose: >>>To calculate aggregate metrics ( eg.totals, average ) for  quick insights 
         >>> to identify overall trends or spot anomalies 

----find the total sales
select * from fact_sales;
select 
      SUM(sales_amount) as  total_sales
from fact_sales 

--Find how many items are sold
select 
      sum(quantity) as total_items 
from fact_sales

--Find the average selling price
select 
      AVG(price) as average_price
from fact_sales

-- Find the Total number of Orders

select 
      count(distinct order_number ) as total_orders
from fact_sales
      
-- Find the total number of products
select * from dim_products
select 
       count( distinct product_number ) as total_product
from dim_products

-- Find the total number of customers
select * from dim_customers;

select 
       COUNT(distinct customer_id ) as total_customers
from dim_customers

-- Find the total number of customers that has placed an order
select 
      COUNT(distinct customer_key) as total_customers
from fact_sales 

-- Generate a Report that shows all key metrics of the business.

select 'total sales' as measure_name , SUM(sales_amount) as measure_value
from fact_sales 
union all
select 'total quantity', SUM(quantity) from fact_sales
union all
select 'average price', AVG(price) from fact_sales
union all
select  'total orders', count(distinct order_number) from fact_sales
union all
select 'total product',count(distinct product_name)  from dim_products
union all
select 'total customers', COUNT(customer_key) from dim_customers
      
############################################################################################################################

Magnitude analysis 

purpose :
        >>> to quantify data and group results by specific dimesions
        >>> for understandiing data distribution acoross categories
sql function used :
  >> aggregate functions 
  >> group by 
############################################################################################################################
-- Find total customers by countries
select * from dim_customers
select 
      country,
      COUNT(distinct customer_id) as total_customers
from dim_customers
group by country
order by total_customers desc;

---Find total customers by gender
select * from dim_customers

select 
      gender,
      COUNT(distinct customer_id) as total_customers
from dim_customers
group by country 
order by total_customers desc

-- Find total products by category
select * from dim_products

select 
       category,
       count( distinct product_id) as total_product
from dim_products
group by category
order by total_product

-- -- What is the average costs in each category?
select * from dim_products
select * from fact_sales

select
      category,
      AVG(cost) as avg_cost
from dim_products 
group by category 
order by avg_cost desc;

-- What is the total revenue generated for each category?

select 
      P.category,
      SUM(f.sales_amount) as total_revenue
from fact_sales f
left join dim_products p
on p.product_key= f.product_key
group by p.category
order by total_revenue


-- what is the  total revenue generated by each customers
select 
      c.customer_key,
      c.first_name,
      c.last_name,
      SUM(f.sales_amount) as total_revenue
from fact_sales f
left join dim_customers c
on c.customer_key = f.customer_key
group by 
c.customer_key,
c.first_name,
c.last_name
order by total_revenue desc;














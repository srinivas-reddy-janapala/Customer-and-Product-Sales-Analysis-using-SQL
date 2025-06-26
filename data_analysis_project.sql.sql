Create database Datawarehouse;

use Datawarehouse;
DROP TABLE IF EXISTS crm_cust_info;

create table crm_cust_info(
cst_id INT,
cst_key VARCHAR(15),
cst_firstname VARCHAR(20),
cst_lastname VARCHAR(20),
cst_marital_status VARCHAR(10),
cst_gndr VARCHAR(10),
cst_create_date DATE
);

DROP TABLE IF EXISTS crm_prd_info;
create table crm_prd_info(
prd_id int ,
prd_key varchar(20),
prd_nm varchar(100),
prd_cost decimal(10,2),
prd_line VARCHAR(10),
prd_start_dt varchar(15),
prd_end_dt varchar(15)
);

DROP TABLE IF EXISTS crm_sales_details;
create table crm_sales_details(
sls_ord_num varchar(10),
sls_prd_key varchar(20),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int
);

DROP TABLE IF EXISTS erp_CUST_AZ12;
create table erp_CUST_AZ12(
CID varchar(20),
BDATE date,
GEN VARCHAR(10)
);

DROP TABLE IF EXISTS erp_LOC_A101;
create table erp_LOC_A101(
CID varchar(20),
CNTRY varchar(20)
);

DROP TABLE IF EXISTS erp_PX_CAT_G1V2;
CREATE TABLE erp_PX_CAT_G1V2 (
  ID VARCHAR(20),
  CAT VARCHAR(20),
  SUBCAT VARCHAR(20),
  MAINTENANCE VARCHAR(10)
);



USE DATAWAREHOUSE;
select * from crm_cust_info;
select * from crm_prd_info;
select * from crm_sales_details;
select * from erp_cust_az12;
select * from erp_loc_a101;
select * from erp_px_cat_g1v2;

# creating new_Database and inserting new data after data cleaning, standadization,normalization and transformtion.
create database new_datawarehouse;

use new_datawarehouse;

DROP TABLE IF EXISTS crm_cust_info;

create table crm_cust_info(
cst_id INT,
cst_key VARCHAR(15),
cst_firstname VARCHAR(20),
cst_lastname VARCHAR(20),
cst_marital_status VARCHAR(10),
cst_gndr VARCHAR(10),
cst_create_date DATE
);

DROP TABLE IF EXISTS crm_prd_info;
create table crm_prd_info(
prd_id int primary key,
cat_id varchar(20),
prd_key varchar(20),
prd_nm varchar(100),
prd_cost int,
prd_line VARCHAR(20),
prd_start_dt DATE,
prd_end_dt DATE
);

DROP TABLE IF EXISTS crm_sales_details;
create table crm_sales_details(
sls_ord_num varchar(10),
sls_prd_key varchar(20),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int
);

DROP TABLE IF EXISTS erp_CUST_AZ12;
create table erp_CUST_AZ12(
CID varchar(20),
BDATE date,
GEN VARCHAR(10)
);

DROP TABLE IF EXISTS erp_LOC_A101;
create table erp_LOC_A101(
CID varchar(20),
CNTRY varchar(20)
);

DROP TABLE IF EXISTS erp_PX_CAT_G1V2;
CREATE TABLE erp_PX_CAT_G1V2 (
  ID VARCHAR(20),
  CAT VARCHAR(20),
  SUBCAT VARCHAR(20),
  MAINTENANCE VARCHAR(10)
);



USE new_DATAWAREHOUSE;
select * from crm_cust_info;
select * from crm_prd_info;
select * from crm_sales_details;
select * from erp_cust_az12;
select * from erp_loc_a101;
select * from erp_px_cat_g1v2;




# checking and cleaning table crm_cust_info
# Checking any duplicate values in the cst_id(primary key)

use datawarehouse;

select cst_id,count(*)
from crm_cust_info
group by cst_id
having count(*) >1;

#checking any unwantwd spaces
#expectation: No results
select cst_firstname
from crm_cust_info
where cst_firstname != trim(cst_firstname);

#checking any unwanted spaces
#expectation: No results
select cst_firstname
from crm_cust_info
where cst_firstname != trim(cst_firstname);

#checking any unwanted spaces
#Expectations: No Results
select cst_gndr
from crm_cust_info
where cst_gndr != trim(cst_gndr);

# Data standardization & Consistency

select distinct(cst_gndr)
from crm_cust_info;

select distinct(cst_marital_status)
from crm_cust_info;

insert into new_datawarehouse.crm_cust_info
select cst_id,cst_key,trim(cst_firstname) as cst_firstname,trim(cst_lastname) as cst_lastname,
case when Upper(trim(cst_marital_status)) ='M' then 'Married'
 when upper(trim(cst_marital_status)) = 'S' then 'Single'
 else 'n/a'
 end cst_marital_status,
case when Upper(trim(cst_gndr)) ='M' then 'Male'
 when upper(trim(cst_gndr)) = 'F' then 'Female'
 else 'n/a'
 end cst_gndr,
cst_create_date
from(
select *, row_number()over(partition by cst_id order by cst_create_date desc) as row_num
from  crm_cust_info) tbl
where row_num= 1;






# checking and cleaning table crm_prd_info
# Checking null & duplicate values in primary key

select prd_id, count(*)
from crm_prd_info
group by prd_id
having count(*) >1;

select prd_id
from crm_prd_info
where prd_id is null;

# checking unwanted spaces from prd_nm column

select prd_nm
from crm_prd_info 
where prd_nm != trim(prd_nm);

#checking any null or negative values in prd_cost

select prd_cost
from crm_prd_info 
where prd_cost != trim(prd_cost);

# Data Normalization and standardization

select distinct(prd_line)
from crm_prd_info ;


insert into new_datawarehouse.crm_prd_info
select 
prd_id,
replace(substring(prd_key,1,5),'-','_')as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
CAST(NULLIF(TRIM(prd_cost), '') AS DECIMAL(10,2)) AS prd_cost,
case when upper(trim(prd_line)) = 'M' then 'Mountain'
when upper(trim(prd_line)) = 'R' then 'Road'
when upper(trim(prd_line)) = 'S' then 'other Sales'
when upper(trim(prd_line)) = 'T' then 'Touring'
else 'n/a'
end prd_line,
cast(prd_start_dt as date) as prd_start_dt ,
 CAST(
    DATE_SUB(
      LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
      INTERVAL 1 DAY
    ) AS DATE
  ) AS prd_end_dt
from crm_prd_info;






# checking data and data cleaning of  table crm_sales_details
# Data cleaning and loading for crm_sales_details table

select * from crm_sales_details;

# Checking any unwanted spaces
select sls_ord_num
from  crm_sales_details
where sls_ord_num != trim(sls_ord_num);

select sls_due_dt
from crm_sales_details
where sls_due_dt is null;

describe crm_sales_details;

# checking invalid date orders

select *
from crm_sales_details
where sls_ship_dt < sls_ord_num or sls_due_dt < sls_ord_num;

# checking data consistency : between sales,quantity,price

select *
from crm_sales_details
where sls_sales is null or sls_sales <0;

select *
from crm_sales_details
where sls_quantity is null or sls_quantity <0;

select *
from crm_sales_details
where sls_price is null or sls_price <0;

select *
from crm_sales_details
where sls_sales != (sls_price * sls_quantity);



insert into new_datawarehouse.crm_sales_details
select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,
abs(sls_quantity * sls_price) as sls_sales,
abs(sls_quantity) as sls_quantity,
abs(sls_price) as sls_price
from crm_sales_details;

# checking data and data cleaning of  table erp_cust_az12
#checking primary key values same as foreign key in another table and rectifiying it.

select 
case when CID like 'NAS%' then substring(CID,4,length(CID)) 
else CID
end CID,BDATE,GEN
from erp_cust_az12;

# Checking out of range dates

select 
BDATE
from erp_cust_az12
where BDATE < '1924-01-01' ;
select 
BDATE
from erp_cust_az12
where BDATE > current_date();

# data Standardization and consistency

select distinct(GEN)
from erp_cust_az12;

insert into new_datawarehouse.erp_cust_az12
select 
case when CID like 'NAS%' then substring(CID,4,length(CID)) 
else CID
end CID,
case when bdate >current_date() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
when upper(trim(gen)) in ('M','MALE') THEN 'Male'
else 'n/a'
end GEN
from erp_cust_az12;


# checking data and data cleaning of  table erp_loc_a101.

insert into  new_datawarehouse.erp_loc_a101
select replace(CID,'-','') as CID,
case when upper(trim(cntry)) in ('US','USA','UNITED STATES') then 'United States'
when upper(trim(cntry)) = 'DE' then 'Germany'
when trim(cntry) = ''  or cntry is null then 'n/a'
else trim(cntry)
end  CNTRY
from erp_loc_a101;

# checking data and data cleaning of  table erp_loc_a101.

insert into new_datawarehouse.erp_px_cat_g1v2
select * from erp_px_cat_g1v2;



select * from new_datawarehouse.crm_cust_info;
select * from new_datawarehouse.erp_cust_az12;
select * from new_datawarehouse.erp_loc_a101;


#  joing customer related tables, checking and cleaning data after joining
#CRM is the master data for gender column


select cst_id, count(*)
from(
select ci.cst_id,ci.cst_key,ci.cst_firstname,ci.cst_lastname,ci.cst_marital_status,ci.cst_gndr,ci.cst_create_date,
ca.bdate,ca.gen,la.cntry
from new_datawarehouse.crm_cust_info as ci
left join new_datawarehouse.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join new_datawarehouse.erp_loc_a101 as la
on ci.cst_key = la.cid) tbl
group by cst_id
having count(*) >1;


select distinct ci.cst_gndr,ca.gen, 
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a') 
end  new_gnder
from new_datawarehouse.crm_cust_info as ci
left join new_datawarehouse.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join new_datawarehouse.erp_loc_a101 as la
on ci.cst_key = la.cid;


use new_datawarehouse; 
create view customers as 
select
 ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a') 
end  gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
from new_datawarehouse.crm_cust_info as ci
left join new_datawarehouse.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join new_datawarehouse.erp_loc_a101 as la
on ci.cst_key = la.cid;

select * from customers;


##  joing product related tables, checking and cleaning data after joining

select * from new_datawarehouse.crm_prd_info;
select * from new_datawarehouse.erp_px_cat_g1v2;

select prd_key, count(*)
from(
select 
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.MAINTENANCE,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
from new_datawarehouse.crm_prd_info as pn
left join new_datawarehouse.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt  is null) as tbl -- filter out all historical data 
group by prd_key
having count(*) >1;


create view  products as 
select 
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.MAINTENANCE as maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from new_datawarehouse.crm_prd_info as pn
left join new_datawarehouse.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt  is null;

select * from products;
select * from crm_sales_details;
select * from customers;


create view sales as
select
sls_ord_num as order_number,
sls_prd_key as product_number,
sls_cust_id as customer_id,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from crm_sales_details;


select * from products;
select * from sales;


# checking foreign key integration
select * from customers as c
 right join  sales as s
 on s.customer_id = c.customer_id
 where c.customer_id is null;
 
 select * from sales as s
 left join products as p
 on s.product_number= p.product_number
 where p.product_number is null;
 
 
# DATABASE EXPLORATION 

# Explore  All objects  in the Database 

 select * 
 from INFORMATION_SCHEMA.TABLES;
 
 # Explore All the Columns in the Database
 
 SELECT * FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME = 'products';
 
  # DIMENSION EXPLORATION
 # Explore all dimensions from table customers.
 select * from customers;
 
 # EXPLORE all countries our customers comes from.
 
 SELECT DISTINCT COUNTRY
 FROM CUSTOMERS;

-- OUR CUSTOMERS ARE FROM 6 COUNTRIES

SELECT GENDER, COUNT(*) as gender_count
FROM CUSTOMERS
GROUP BY GENDER;
-- Males are sightly more than female

SELECT MARITAL_STATUS, COUNT(*)
FROM CUSTOMERS
GROUP BY MARITAL_STATUS;

--- MARRIED PEOPLE ARE MORE THAN SINGLE

SELECT MIN(BIRTH_DATE) AS YOUNGEST , MAX(BIRTH_DATE) AS OLDEST,
ROUND(DATEDIFF(CURRENT_DATE,MIN(BIRTH_DATE))/365) AS OLDEST_AGE,
ROUND(DATEDIFF(CURRENT_DATE,MAX(BIRTH_DATE))/365) AS YOUNGEST_AGE
FROM CUSTOMERS;


# EXPLORE  CATEGORIES columns from table products
select * from products;

select distinct category
from products;
select distinct subcategory
from products;
select distinct product_line
from products;
# check the granularity of the category of the product
select distinct category,subcategory,product_name
from products
order by category,subcategory,product_name;

# Find the min cost and max_cost od product
select min(cost) min_price,max(cost) as max_price
from products;

#Explore Category columns from sales

# FIND THE FIRST AND LAST ORDER DATE AND DIFF IN MONTHS

SELECT   MIN(ORDER_DATE)FIRST_ORDER_DATE,
 MAX(ORDER_DATE) LAST_ORDER_DATE,
ROUND(datediff(MAX(ORDER_DATE), MIN(ORDER_DATE))/30)ORDER_RANGE_months
FROM SALES;



# Explore Measures from the three tables.

# Find the total sales

select sum(sales_amount) as total_sales
from sales;

# Find the total items sold
select sum(quantity)as total_quantity_sold
from sales;

# find the avg selling price of item.

select avg(price) as avg_selling_price
from sales;

# Total avg selling price sold.

select sum(sales_amount)/sum(quantity) as avg_selling_price
from sales;

# find the total number of orders

select count(order_number) as total_orders from sales;
select count(distinct order_number) as total_orders from sales;

# find the total number of products

select count(product_number) as total_products from products;
select count(distinct product_number) as total_products from products;

# find the total number of customers

select count(customer_id) as total_customers from customers;

# Find the total number of customer who placed atleast one order.
select count(distinct c.customer_id) as total_customers from customers as c
inner join sales as s 
on c.customer_id= s.customer_id;

#Generate report that shows all key metric of the business


select 'Total Sales' as measure_name ,sum(sales_amount) as total_sales from sales
union all 
select 'Total Quantity' as measure_name ,sum(quantity) as total_sales from sales
union all
select 'Average price' as measure_name,avg(price) as avg_selling_price from sales
union all 
select 'Total orders' as measure_name,count(distinct order_number) as total_orders from sales
union all
select 'Total  products' as measure_name ,count(distinct product_number) as total_products from products
union all
select 'Total customers' as measure_name,count(customer_id) as total_customers from customers;

# lets do some EDA.

#Find total customers by countries.

select country,count(customer_number)as total_customers
from customers
group by  country;

#Find total customers by Gender.

select Gender,count(customer_number)as total_customers
from customers
group by  Gender;

#find total product by category.



select category,count(product_number) as total_products
from products
group by category;

# what is the avg cost in each category

select category,avg(cost) as avg_cost
from products
group by category
order by avg_cost desc;

# find the total revenue generated by each category

select * from products;
select * from sales;

select p.category,sum(s.sales_amount) as total_revenue_per_category
from products as p
left join sales as s
on p.product_number = s.product_number
group by p.category
order by total_revenue_per_category desc;


select p.category,sum(s.sales_amount) as total_revenue_per_category
from products as p
inner join sales as s
on p.product_number = s.product_number
group by p.category
order by total_revenue_per_category desc;


#Total revenue generated by each customer.

select c.customer_id,c.first_name,c.last_name,sum(s.sales_amount)as total_revenue_per_customer
from customers as c
left join sales as s
on c.customer_id = s.customer_id
group by c.customer_id,c.first_name,c.last_name
order by total_revenue_per_customer desc;

# what  is the distribution of sold items across countries.

select c.country,sum(quantity) as total_sold_items
from customers as c
left join sales as s
on c.customer_id = s.customer_id
group by c.country;

# Find total number of quantity purchased based on gender.

select Gender,sum(quantity)
from customers as c
left join sales as s
on c.customer_id = s.customer_id
group by Gender;

# Top 5 products that generated highest revenue.


select  p.product_name,sum(s.sales_amount)as total_sales
from sales as s
inner join products as p
group by p.product_name
order by total_sales desc
limit 5 ;

select *
from(
select  p.product_name,sum(s.sales_amount)as total_sales,row_number()over(order by sum(s.sales_amount) desc) as rnk_products
from sales as s
inner join products as p
on s.product_number = p.product_number
group by p.product_name) as tbl
where rnk_products <=5;


# Bottom 5 products that generated highest revenue.


select  p.product_name,sum(s.sales_amount)as total_sales
from sales as s
inner join products as p
on s.product_number = p.product_number
group by p.product_name
order by total_sales 
limit 5 ;

# Find the top 10 customers with highest revenue. 


select *
from(
select c.customer_id,sum(s.sales_amount) as revenue_per_customer,
dense_rank()over(order by sum(s.sales_amount) desc) as rnk
from customers as c
inner join sales as s
on c.customer_id= s.customer_id 
group by c.customer_id) as tbl
where rnk <=10;


# Advance data analytics.
# change over time

select year(order_date) as year,sum(sales_amount) as total_amount, count(distinct customer_id) as total_customers,
sum(quantity) as total_quantity
from sales
group by year(order_date)
order by year;

select month(order_date) as month,sum(sales_amount) as total_amount, count(distinct customer_id) as total_customers,
sum(quantity) as total_quantity
from sales
group by month(order_date)
order by month;

select year(order_date) as year, month(order_date) as month,sum(sales_amount) as total_amount, 
count(distinct customer_id) as total_customers,
sum(quantity) as total_quantity
from sales
group by year(order_date) , month(order_date)
order by year ,month;


select date_format(order_date,'%Y-%m-01') as year, sum(sales_amount) as total_amount, 
count(distinct customer_id) as total_customers,
sum(quantity) as total_quantity
from sales
group by date_format(order_date,'%Y-%m-01')
order by year;

# cummulative_analysis.

# Calculate the total sales per month

select *,
sum(total_sales)over(partition by year order by year) as running_total_sales
from(
select date_format(order_date,'%Y-%m-01') as year, sum(sales_amount) as total_sales
from sales
group by date_format(order_date,'%Y-%m-01')
order by year) as tbl;


select *,
sum(total_sales)over(order by year) as running_total_sales,
avg(avg_price)over(order by year) as moving_avg_price
from(
select year(order_date) as year, sum(sales_amount) as total_sales,avg(price) as avg_price
from sales
group by year(order_date)
order by year) as tbl;

# performance analysis


/*Analyse the yearly performance of products by comparing the sales to both the avg sales performance 
of the product and the previous year sales*/


with yearly_product_sales as (
select year(s.order_date) as year,p.product_name,sum(s.sales_amount) as total_product_sales
from products as p
inner join sales as s 
on p.product_number = s.product_number 
group by year(s.order_date),p.product_name)
select year,product_name,total_product_sales,
avg(total_product_sales)over(partition by product_name) avg_sales,
total_product_sales-avg(total_product_sales)over(partition by product_name) as diff_avg,
case when total_product_sales-avg(total_product_sales)over(partition by product_name) > 0 then 'above_avg'
when total_product_sales-avg(total_product_sales)over(partition by product_name) < 0 then 'below_avg'
else 'avg'end as avg_change,
lag(total_product_sales)over(partition by product_name order by year) as previous_year,
total_product_sales-lag(total_product_sales)over(partition by product_name order by year) as sales_diff,
case when total_product_sales-lag(total_product_sales)over(partition by product_name order by year) > 0 then 'Increase'
when total_product_sales-lag(total_product_sales)over(partition by product_name order by year) < 0 then 'decrease'
else 'no change'end as py_chnage
from yearly_product_sales
order by product_name,year;


# Part to whole analysis

# Which category contribute the most to overall sales.

select * from sales;
select * from products;

with cte as(
select p.category,sum(sales_amount) as total_sales_amount_per_category
from products as p 
inner join sales as s 
on p.product_number = s.product_number
group by p.category)
select *,sum(total_sales_amount_per_category)over() as total_sales,
concat(round(total_sales_amount_per_category/sum(total_sales_amount_per_category)over()*100,2),'%') percentage_of_total
from cte ;

# Data segmentation
/* segment product into cost ranges and count how many products fall into each segmewnt*/


with cte as(
select product_id,product_name,cost,
case when cost <100 then 'below 100'
when cost between 100 and 500 then 'below 100-500'
when cost between 500 and 1000 then 'below 500-1000'
else 'above 1000'
end cost_range
from products)
select 
cost_range,
count(product_id) as total_products
from cte
group by cost_range;




with cte as(
select c.customer_id,sum(s.sales_amount) total_spending,min(order_date) as  first_order,max(order_date)as recent_order,
timestampdiff(month,min(order_date),max(order_date)) as months
from customers as c
inner join sales as s
on c.customer_id = s.customer_id
group by customer_id),
cte2 as
(select *,
case when months >12 and total_spending >5000 then 'VIP'
when months >12 and total_spending <5000 then 'Regular'
when months <12  then 'New'
end customer_segmentation
from cte)
select customer_segmentation,count(customer_segmentation) as customers
 from cte2
 group by customer_segmentation;



create view Report as
(
with base_query as(
select c.customer_id,c.customer_number,concat(c.first_name,' ',c.last_name) as customer_name,
timestampdiff(year,c.birth_date,curdate())as age,s.order_number,s.product_number,s.order_date,s.quantity,
s.sales_amount
from customers as c
inner join sales as s 
on c.customer_id = s.customer_id),
customer_aggregations as (
select
customer_id,customer_number,customer_name,age,count(distinct order_number) as orders,sum(quantity) as total_quantity,sum(sales_amount)as total_sales,
max(order_date)as recent_order,
timestampdiff(month,min(order_date),max(order_date)) as life_span
from base_query
group by customer_id,customer_number,customer_name,age)

select 
customer_id, customer_number,customer_name,age,orders,total_quantity,total_sales,recent_order,
timestampdiff(month,recent_order,curdate()) as recency,
life_span,
case when age <20 then 'under 20'
 when age between 20 and 29  then '20-29'
  when age between 30 and 39  then '30-39'
when age between 40 and 49  then '40-49'
else '50 and above'
end as age_group,
case when life_span >12 and total_sales >5000 then 'VIP'
when life_span >12 and total_sales <5000 then 'Regular'
when life_span <12  then 'New' 
end as customer_segmentation,
round(total_sales/orders,2) as avg_order_value,
round(total_sales/life_span,2)as avg_montly_spend
from customer_aggregations);




create view product_segmentation as (
with product_details as (
select p.product_id,p.product_number,p.product_name,p.category,p.subcategory,p.cost,s.order_number,s.customer_id,
s.order_date,s.sales_amount,s.quantity
from products as p
inner join sales as s
on p.product_number = s.product_number),
product_aggregates as(
select product_id,product_number,product_name,
count(distinct order_number) as total_orders,sum(sales_amount) as total_revenue,sum(quantity)as total_quantity,count(distinct customer_id) as customers,
max(order_date)as last_order_date,
timestampdiff(month,min(order_date),max(order_date)) as life_span,
sum(sales_amount) /sum(quantity) as avg_selling_price
from product_details
group by product_id,product_number,product_name)
select 
product_id,product_number,product_name,total_orders, total_revenue,total_quantity,
timestampdiff(month,last_order_date,curdate()) as recency_months,life_span,avg_selling_price,customers,
case when total_revenue > 5000 then 'High performance'
when total_revenue between 10000 and 50000 then 'Mid range'
else 'low-performance'
end as product_segment,
total_revenue/total_orders as avg_order_revenue,
total_revenue/life_span as avg_monthly_revenue
from product_aggregates);
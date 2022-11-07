# Create the staging schema (Note: Bigquery usually calls it a dataset
create schema if not exists staging;

create or replace table staging.repl_customer_wealth_brackets
(id int,
 name string,
 description string,
 lastmodifieddate datetime,
 createddate datetime);

 insert into staging.repl_customer_wealth_brackets
 values
   (1,'Whale','A customer who is responsible for over 5% of our total sales', current_datetime(), current_datetime()),
   (2,'VIP','A customer who is responsible for over 1% of our total sales', current_datetime(), current_datetime()),
   (3,'High Value Buyer', 'A regular customer who buys more than our threshold value of goods per month', current_datetime(), current_datetime()),
   (4,'Low Value Buyer', 'A regular customer who buys less than our threshold value of goods per month', current_datetime(), current_datetime()),
   (5,'Infrequent Buyer', 'A customer who purchases goods, on average, less than once every 3 months', current_datetime(), current_datetime());

# Create the replica of source tables
CREATE OR REPLACE TABLE staging.repl_UK_customers
(
  customer_id STRING ,
  customer_number STRING,
  first_name STRING,
  last_name STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  applieddate DATETIME,
  CRUD_flag STRING
);

alter table staging.repl_UK_customers add column title string;
alter table staging.repl_UK_customers add column wealth_bracket int;

# Create the tables - this is what dbtvault calls raw staging
CREATE OR REPLACE TABLE staging.repl_US_customers
(
  customer_id STRING ,
  customer_number STRING,
  first_name STRING,
  last_name STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  applieddate DATETIME,
  CRUD_flag STRING
);

alter table staging.repl_US_customers add column title string;
alter table staging.repl_US_customers add column wealth_bracket int;

CREATE OR REPLACE TABLE staging.repl_UK_orders
(
  order_id STRING,
  order_number STRING,
  order_price DECIMAL,
  customer_id STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  CRUD_flag STRING
);


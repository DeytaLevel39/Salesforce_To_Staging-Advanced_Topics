# Create the staging schema (Note: Bigquery usually calls it a dataset
create schema if not exists staging;

# Create the tables - this is what dbtvault calls raw staging
CREATE OR REPLACE TABLE staging.repl_uk_customers
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

# Create the tables - this is what dbtvault calls raw staging
CREATE OR REPLACE TABLE staging.repl_us_customers
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

CREATE OR REPLACE TABLE staging.repl_orders
(
  order_id STRING,
  order_number STRING,
  order_price DECIMAL,
  customer_id STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  CRUD_flag STRING
);


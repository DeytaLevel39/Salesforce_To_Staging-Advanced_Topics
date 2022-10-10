# Create the staging schema (Note: Bigquery usually calls it a dataset
create schema if not exists staging;

# Create the tables - this is what dbtvault calls raw staging
CREATE OR REPLACE TABLE staging.repl_customers
(
  customer_id STRING ,
  customer_number STRING,
  first_name STRING,
  last_name STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
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

CREATE OR REPLACE TABLE staging.repl_line_items
(
  order_id STRING,
  line_item_id STRING,
  line_number INT,
  product_id STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  CRUD_flag STRING
);

CREATE OR REPLACE TABLE staging.repl_products
(product_id STRING,
 product_number STRING,
 product_name STRING,
 unit_price DECIMAL,
 lastmodifieddate DATETIME,
 createddate DATETIME,
 CRUD_flag STRING
);



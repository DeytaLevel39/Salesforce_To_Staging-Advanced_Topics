# Create the staging schema (Note: Bigquery usually calls it a dataset
create schema if not exists staging;

# Create the tables - this is what dbtvault calls raw staging
CREATE OR REPLACE TABLE staging.customers
(
  customer_id STRING ,
  customer_number STRING,
  first_name STRING,
  last_name STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  loaddate DATETIME
);

CREATE OR REPLACE TABLE staging.orders
(
  order_id STRING,
  order_number STRING,
  line_item_id STRING,
  customer_id STRING,
  product_id STRING,
  lastmodifieddate DATETIME,
  createddate DATETIME,
  loaddate DATETIME
);

create or replace table staging.products
(product_id STRING,
 product_number STRING,
 product_name STRING,
 unit_price decimal,
 lastmodifieddate DATETIME,
 createddate DATETIME,
 loaddate DATETIME
);

#Create the views that add the derived & hashed columns
#dbtvault refers to this as hashed_staging

Create or replace view staging.v_customers as
    Select customer_id, customer_number, first_name, last_name, lastmodifieddate, createddate, loaddate, 'Salesforce' as record_source, lastmodifieddate as effective_from,
    CAST((SHA1(NULLIF(UPPER(TRIM(CAST(CUSTOMER_NUMBER AS STRING))), ''))) AS BYTES) AS CUSTOMER_HK,
    CAST(SHA1(CONCAT( IFNULL(NULLIF(UPPER(TRIM(CAST(CUSTOMER_NUMBER AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_NAME AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_NAME AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(LASTMODIFIEDDATE AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(CREATEDDATE AS STRING))), ''), '^^') )) AS BYTES) AS HASHDIFF
    from staging.customers;

Create or replace view staging.v_orders as
    Select order_id, order_number, line_item_id, customer_id, product_id, lastmodifieddate, createddate, loaddate, 'Salesforce' as record_source, lastmodifieddate as effective_from,
    CAST((SHA1(NULLIF(UPPER(TRIM(CAST(ORDER_NUMBER AS STRING))), ''))) AS BYTES) AS ORDER_HK,
    CAST(SHA1(CONCAT( IFNULL(NULLIF(UPPER(TRIM(CAST(ORDER_NUMBER AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(ORDER_ID AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(CUSTOMER_ID AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(PRODUCT_ID AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(LASTMODIFIEDDATE AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(CREATEDDATE AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(LINE_ITEM_ID AS STRING))), ''), '^^') )) AS BYTES) AS HASHDIFF
    from staging.orders;

Create or replace view staging.v_products as
    Select product_id, product_number, product_name, unit_price, lastmodifieddate, createddate, loaddate, 'Salesforce' as record_source, lastmodifieddate as effective_from,
    CAST((SHA1(NULLIF(UPPER(TRIM(CAST(PRODUCT_NUMBER AS STRING))), ''))) AS BYTES) AS ORDER_HK,
    CAST(SHA1(CONCAT( IFNULL(NULLIF(UPPER(TRIM(CAST(PRODUCT_NUMBER AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(PRODUCT_ID AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(PRODUCT_NAME AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(LASTMODIFIEDDATE AS STRING))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST(CREATEDDATE AS STRING))), ''), '^^') )) AS BYTES) AS HASHDIFF
    from staging.products;

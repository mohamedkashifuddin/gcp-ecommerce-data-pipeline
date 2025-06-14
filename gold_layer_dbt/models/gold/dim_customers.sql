-- models/gold/dim_customers.sql

{{ config(materialized='table') }}

SELECT
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    join_date,
    ingestion_timestamp,
    source_table
FROM
    {{ source('batch_pipeline_silver', 'customers') }}
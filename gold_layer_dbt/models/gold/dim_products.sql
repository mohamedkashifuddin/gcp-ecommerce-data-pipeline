-- models/gold/dim_products.sql

{{ config(
    materialized='table'
) }}

SELECT
    product_id,
    product_name,
    category AS product_category,
    price AS product_price
FROM
    {{ source('batch_pipeline_silver', 'products') }}
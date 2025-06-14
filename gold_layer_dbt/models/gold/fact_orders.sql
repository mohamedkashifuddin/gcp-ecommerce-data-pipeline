-- models/gold/fact_orders.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.total_amount AS order_total_amount,
    oi.order_item_id,
    oi.product_id,
    oi.quantity AS order_item_quantity,
    oi.price AS order_item_unit_price,
    oi.item_total AS order_item_total_amount,
    p.product_name,
    p.category AS product_category,
    p.price AS product_list_price,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.full_name AS customer_full_name,
    c.email AS customer_email,
    c.join_date AS customer_join_date,
    d.day_of_week AS order_day_of_week,
    d.month AS order_month,
    d.year AS order_year,
    pay.payment_id,
    pay.payment_date,
    pay.payment_amount,
    -- ADD THESE LINES:
    o.ingestion_timestamp,
    o.source_table
FROM
    {{ source('batch_pipeline_silver', 'orders') }} AS o

JOIN
    {{ source('batch_pipeline_silver', 'order_items') }} AS oi
    ON o.order_id = oi.order_id

LEFT JOIN
    {{ source('batch_pipeline_silver', 'products') }} AS p
    ON oi.product_id = p.product_id

LEFT JOIN
    {{ source('batch_pipeline_silver', 'customers') }} AS c
    ON o.customer_id = c.customer_id

LEFT JOIN
    {{ source('batch_pipeline_silver', 'dates') }} AS d
    ON o.order_date = d.date

LEFT JOIN
    {{ source('batch_pipeline_silver', 'payments') }} AS pay
    ON o.order_id = pay.order_id

{% if is_incremental() %}
WHERE o.order_date > COALESCE((SELECT MAX(order_date) FROM {{ this }}), '1900-01-01')
{% endif %}






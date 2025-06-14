-- models/gold/dim_dates.sql

{{ config(
    materialized='table'
) }}

SELECT
    date AS date_key,
    day_of_week,
    month AS month_number,
    year AS year_number
FROM
    {{ source('batch_pipeline_silver', 'dates') }}
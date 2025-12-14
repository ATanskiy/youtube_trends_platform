{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='category_id',
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        id          AS category_id,
        name        AS category_name,
        created_at
    FROM {{ source('bronze', 'categories') }}
)

SELECT *
FROM src
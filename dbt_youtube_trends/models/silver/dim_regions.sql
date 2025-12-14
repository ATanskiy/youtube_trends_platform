{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='region_id',
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        id          AS region_id,
        name        AS region_name,
        created_at
    FROM {{ source('bronze', 'regions') }}
)

SELECT *
FROM src
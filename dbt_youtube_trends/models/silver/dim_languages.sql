{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='language_id',
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        id          AS language_id,
        name        AS language_name,
        created_at
    FROM {{ source('bronze', 'languages') }}
)

SELECT *
FROM src
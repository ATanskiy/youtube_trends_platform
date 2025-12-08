{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    table_type='iceberg',
    on_schema_change='sync_all_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'users_raw') }}
),

parsed AS (
    SELECT
        get_json_object(json_raw, '$.user_id')              AS user_id,
        CAST(get_json_object(json_raw, '$.created_at') AS TIMESTAMP) AS created_at,
        get_json_object(json_raw, '$.password')             AS password,
        inserted_at
    FROM src
)

SELECT * FROM parsed
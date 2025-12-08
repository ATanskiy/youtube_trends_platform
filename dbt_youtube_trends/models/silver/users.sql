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
        get_json_object(json_raw, '$.user_id')       AS user_id,
        get_json_object(json_raw, '$.gender')        AS gender,
        get_json_object(json_raw, '$.first_name')    AS first_name,
        get_json_object(json_raw, '$.last_name')     AS last_name,
        inserted_at
    FROM src
)

SELECT * FROM parsed
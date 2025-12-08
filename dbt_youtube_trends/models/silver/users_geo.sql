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
        get_json_object(json_raw, '$.user_id') AS user_id,
        get_json_object(json_raw, '$.city')     AS city,
        get_json_object(json_raw, '$.state')    AS state,
        get_json_object(json_raw, '$.state_id') AS state_id,
        get_json_object(json_raw, '$.postcode') AS postcode,
        CAST(get_json_object(json_raw, '$.latitude') AS DOUBLE)   AS latitude,
        CAST(get_json_object(json_raw, '$.longitude') AS DOUBLE)  AS longitude,
        inserted_at
    FROM src
)

SELECT * FROM parsed
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
        get_json_object(json_raw, '$.user_id')            AS user_id,
        get_json_object(json_raw, '$.picture_large')      AS picture_large,
        get_json_object(json_raw, '$.picture_medium')     AS picture_medium,
        get_json_object(json_raw, '$.picture_thumbnail')  AS picture_thumbnail,
        inserted_at
    FROM src
)

SELECT * FROM parsed
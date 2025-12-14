{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='channel_id',
    table_type='iceberg'
) }}

WITH ranked AS (
    SELECT
        channel_id,
        channel_title,
        created_at AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY channel_id
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ source('bronze', 'videos') }}
)

SELECT
    channel_id,
    channel_title,
    updated_at
FROM ranked
WHERE rn = 1
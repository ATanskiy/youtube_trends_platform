{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='video_id',
    table_type='iceberg'
) }}

WITH ranked AS (
    SELECT
        id AS video_id,
        title,
        description,
        duration,
        published_at,
        category_id,
        channel_id,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ source('bronze', 'videos') }}
)

SELECT
    video_id,
    title,
    description,
    duration,
    published_at,
    category_id,
    channel_id
FROM ranked
WHERE rn = 1
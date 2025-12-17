{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='video_id',
    table_type='iceberg'
) }}

WITH source_data AS (

    SELECT
        id AS video_id,
        title,
        description,
        duration,
        CAST(published_at AS TIMESTAMP) AS published_at,
        category_id,
        channel_id,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM {{ source('bronze', 'videos') }}

),

max_loaded AS (

    {% if is_incremental() %}
        SELECT MAX(created_at) AS max_created_at
        FROM {{ this }}
    {% else %}
        SELECT TIMESTAMP '1970-01-01' AS max_created_at
    {% endif %}

),

filtered AS (

    SELECT s.*
    FROM source_data s
    CROSS JOIN max_loaded m
    WHERE s.created_at > m.max_created_at

),

ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY created_at DESC
        ) AS rn
    FROM filtered

)

SELECT
    video_id,
    title,
    description,
    duration,
    published_at,
    category_id,
    channel_id,
    created_at
FROM ranked
WHERE rn = 1
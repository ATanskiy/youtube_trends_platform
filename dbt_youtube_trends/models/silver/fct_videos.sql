{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['video_id', 'region_id', 'snapshot_at'],
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        id                  AS video_id,

        region_id,
        language_id,

        channel_id,
        category_id,

        CAST(view_count AS BIGINT)      AS view_count,
        CAST(like_count AS BIGINT)      AS like_count,
        CAST(favorite_count AS BIGINT)  AS favorite_count,
        CAST(comment_count AS BIGINT)   AS comment_count,
        CAST(published_at AS DATE)      AS published_at,
        CAST(created_at AS DATE)        AS snapshot_at
    FROM {{ source('bronze', 'videos') }}
)

SELECT *
FROM src

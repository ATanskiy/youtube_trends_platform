{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['video_id', 'region_id', 'snapshot_at'],
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        id AS video_id,
        region_id,
        language_id,
        language_id_src,
        channel_id,
        category_id,
        CAST(view_count AS BIGINT)      AS view_count,
        CAST(like_count AS BIGINT)      AS like_count,
        CAST(favorite_count AS BIGINT)  AS favorite_count,
        CAST(comment_count AS BIGINT)   AS comment_count,
        CAST(published_at AS TIMESTAMP) AS published_at,
        CAST(created_at AS TIMESTAMP)   AS snapshot_at
    FROM {{ source('bronze', 'videos') }}
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY video_id, region_id, snapshot_at
                ORDER BY snapshot_at DESC
            ) AS rn
        FROM src
    )
    WHERE rn = 1
)

SELECT
    video_id,
    region_id,
    language_id,
    language_id_src,
    channel_id,
    category_id,
    view_count,
    like_count,
    favorite_count,
    comment_count,
    published_at,
    snapshot_at
FROM deduped

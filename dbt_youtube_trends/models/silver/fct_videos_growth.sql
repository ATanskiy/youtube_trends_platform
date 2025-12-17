{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['video_id', 'region_id', 'snapshot_at'],
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

WITH base AS (
    SELECT
        video_id,
        region_id,
        language_id,
        language_id_src,
        channel_id,
        category_id,
        published_at,
        snapshot_at,
        view_count,
        like_count,
        favorite_count,
        comment_count
    FROM {{ ref('fct_videos') }}

),

{% if is_incremental() %}
    history AS (
    SELECT
        video_id,
        region_id,
        language_id,
        language_id_src,
        channel_id,
        category_id,
        published_at,
        snapshot_at,
        view_count,
        like_count,
        favorite_count,
        comment_count
    FROM {{ this }}
    UNION ALL
    SELECT
        video_id,
        region_id,
        language_id,
        language_id_src,
        channel_id,
        category_id,
        published_at,
        snapshot_at,
        view_count,
        like_count,
        favorite_count,
        comment_count
    FROM base
    WHERE snapshot_at > (SELECT MAX(snapshot_at) FROM {{ this }})

),
{% else %}
    history AS (
        SELECT * FROM base
),
{% endif %}

growth AS (
    SELECT
        *,
        CAST(view_count - LAG(view_count) 
            OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)
            AS BIGINT
        ) AS view_growth,

        CAST(like_count - LAG(like_count) 
            OVER ( PARTITION BY video_id, region_id ORDER BY snapshot_at) AS BIGINT) 
            AS like_growth,

        CAST(favorite_count - LAG(favorite_count) 
            OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)AS BIGINT) 
            AS favorite_growth,

        CAST(comment_count - LAG(comment_count) 
            OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)AS BIGINT) 
            AS comment_growth
    FROM history
)

SELECT *
FROM growth

{% if is_incremental() %}
    WHERE snapshot_at > (SELECT MAX(snapshot_at) FROM {{ this }})
{% endif %}
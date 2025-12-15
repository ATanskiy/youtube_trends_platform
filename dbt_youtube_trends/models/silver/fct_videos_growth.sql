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

    {% if is_incremental() %}
      WHERE snapshot_at > (
          SELECT COALESCE(MAX(snapshot_at), TIMESTAMP '1970-01-01')
          FROM {{ this }}
      )
    {% endif %}
),

with_history AS (

    SELECT * FROM base

    {% if is_incremental() %}
    UNION ALL
    SELECT
        video_id,
        region_id,
        language_id,
        language_id_src
        channel_id,
        category_id,
        published_at,
        snapshot_at,
        view_count,
        like_count,
        favorite_count,
        comment_count
    FROM {{ this }}
    {% endif %}
),

growth AS (

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
        comment_count,

        -- view growth
        CASE
            WHEN LAG(view_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at) IS NULL
            THEN 0
            ELSE view_count - LAG(view_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)
        END AS view_growth,

        -- like growth
        CASE
            WHEN LAG(like_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at) IS NULL
            THEN 0
            ELSE like_count - LAG(like_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)
        END AS like_growth,

        -- favorite growth
        CASE
            WHEN LAG(favorite_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at) IS NULL
            THEN 0
            ELSE favorite_count - LAG(favorite_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)
        END AS favorite_growth,

        -- comment growth
        CASE
            WHEN LAG(comment_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at) IS NULL
            THEN 0
            ELSE comment_count - LAG(comment_count) OVER (PARTITION BY video_id, region_id ORDER BY snapshot_at)
        END AS comment_growth
    FROM with_history
)

SELECT *
FROM growth
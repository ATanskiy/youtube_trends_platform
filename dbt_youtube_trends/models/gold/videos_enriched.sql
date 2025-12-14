{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['video_id', 'region_id', 'snapshot_at'],
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

SELECT
    -- keys
    g.video_id,
    g.region_id,
    g.language_id,
    g.channel_id,
    g.category_id,

    -- time
    g.snapshot_at,
    DATE(g.snapshot_at) AS snapshot_date,
    v.published_at       AS video_published_at,

    -- metrics (absolute)
    g.view_count,
    g.like_count,
    g.favorite_count,
    g.comment_count,

    -- metrics (growth)
    g.view_growth,
    g.like_growth,
    g.favorite_growth,
    g.comment_growth,

    -- video attributes
    v.title,
    v.description,
    v.duration,

    -- channel
    ch.channel_title,

    -- dimensions
    c.category_name,
    l.language_name,
    r.region_name

FROM {{ ref('fct_videos_growth') }} g

LEFT JOIN {{ ref('dim_videos') }} v
    ON g.video_id = v.video_id

LEFT JOIN {{ ref('dim_channels') }} ch
    ON g.channel_id = ch.channel_id

LEFT JOIN {{ ref('dim_categories') }} c
    ON g.category_id = c.category_id

LEFT JOIN {{ ref('dim_languages') }} l
    ON g.language_id = l.language_id

LEFT JOIN {{ ref('dim_regions') }} r
    ON g.region_id = r.region_id
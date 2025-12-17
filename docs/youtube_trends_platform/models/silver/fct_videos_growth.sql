

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
    FROM silver.fct_videos

    
      WHERE snapshot_at > (
          SELECT COALESCE(MAX(snapshot_at), TIMESTAMP '1970-01-01')
          FROM silver.fct_videos_growth
      )
    
),

with_history AS (

    SELECT * FROM base

    
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
    FROM silver.fct_videos_growth
    
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
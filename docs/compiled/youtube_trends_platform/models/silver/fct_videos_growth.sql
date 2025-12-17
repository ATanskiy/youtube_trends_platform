

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

),


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
    FROM silver.fct_videos_growth
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
    WHERE snapshot_at > (SELECT MAX(snapshot_at) FROM silver.fct_videos_growth)

),


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


    WHERE snapshot_at > (SELECT MAX(snapshot_at) FROM silver.fct_videos_growth)

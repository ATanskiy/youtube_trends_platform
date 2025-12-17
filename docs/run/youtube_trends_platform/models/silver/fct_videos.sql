
  
    
        create or replace table silver.fct_videos
      
      
    using iceberg
      
      
      
      
      
      

      as
      

WITH src AS (
    SELECT
        id                  AS video_id,

        region_id,
        language_id,
        language_id_src,

        channel_id,
        category_id,

        CAST(view_count AS BIGINT)      AS view_count,
        CAST(like_count AS BIGINT)      AS like_count,
        CAST(favorite_count AS BIGINT)  AS favorite_count,
        CAST(comment_count AS BIGINT)   AS comment_count,
        CAST(published_at AS TIMESTAMP)      AS published_at,
        CAST(created_at  AS TIMESTAMP)      AS snapshot_at
    FROM bronze.videos
)

SELECT *
FROM src
  
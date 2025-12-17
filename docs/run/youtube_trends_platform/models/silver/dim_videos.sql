
  
    
        create or replace table silver.dim_videos
      
      
    using iceberg
      
      
      
      
      
      

      as
      

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
            ORDER BY CAST(created_at AS TIMESTAMP) DESC
        ) AS rn
    FROM bronze.videos
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
  
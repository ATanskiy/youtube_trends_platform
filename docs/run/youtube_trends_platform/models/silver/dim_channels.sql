
  
    
        create or replace table silver.dim_channels
      
      
    using iceberg
      
      
      
      
      
      

      as
      

WITH ranked AS (
    SELECT
        channel_id,
        channel_title,
        created_at AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY channel_id
            ORDER BY created_at DESC
        ) AS rn
    FROM bronze.videos
)

SELECT
    channel_id,
    channel_title,
    updated_at
FROM ranked
WHERE rn = 1
  
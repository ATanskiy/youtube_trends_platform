
  
    
        create or replace table silver.dim_regions
      
      
    using iceberg
      
      
      
      
      
      

      as
      

WITH src AS (
    SELECT
        id          AS region_id,
        name        AS region_name,
        created_at
    FROM bronze.regions
)

SELECT *
FROM src
  
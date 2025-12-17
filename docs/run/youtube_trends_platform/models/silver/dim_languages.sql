
  
    
        create or replace table silver.dim_languages
      
      
    using iceberg
      
      
      
      
      
      

      as
      

WITH src AS (
    SELECT
        id          AS language_id,
        name        AS language_name,
        created_at
    FROM bronze.languages
)

SELECT *
FROM src
  
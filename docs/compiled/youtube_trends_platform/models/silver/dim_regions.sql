

WITH src AS (
    SELECT
        id          AS region_id,
        name        AS region_name,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM bronze.regions
)

SELECT *
FROM src
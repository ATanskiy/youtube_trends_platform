

WITH src AS (
    SELECT
        id          AS category_id,
        name        AS category_name,
        CAST(created_at AS TIMESTAMP) AS   created_at
    FROM bronze.categories
)

SELECT *
FROM src
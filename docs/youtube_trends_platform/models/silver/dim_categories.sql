

WITH src AS (
    SELECT
        id          AS category_id,
        name        AS category_name,
        created_at
    FROM bronze.categories
)

SELECT *
FROM src
{{ config(
    materialized='table',
    file_format='iceberg',
    table_type='iceberg',
    on_schema_change='sync_all_columns'
) }}

WITH b AS (SELECT * FROM {{ ref('users') }}),
     c AS (SELECT * FROM {{ ref('users_contact') }}),
     p AS (SELECT * FROM {{ ref('users_picture') }}),
     g AS (SELECT * FROM {{ ref('users_geo') }}),
     m AS (SELECT * FROM {{ ref('users_metadata') }})

SELECT
    b.user_id,
    b.gender,
    b.first_name,
    b.last_name,
    CONCAT(b.first_name, ' ', b.last_name) AS full_name,

    c.email,
    c.phone,
    c.cell,

    p.picture_large,
    p.picture_medium,
    p.picture_thumbnail,

    g.city,
    g.state,
    g.state_id,
    g.postcode,
    g.latitude,
    g.longitude,

    m.created_at,
    b.inserted_at,

    FLOOR(DATEDIFF(current_date, CAST(m.created_at AS DATE)) / 365) AS age

FROM b
LEFT JOIN c ON b.user_id = c.user_id
LEFT JOIN p ON b.user_id = p.user_id
LEFT JOIN g ON b.user_id = g.user_id
LEFT JOIN m ON b.user_id = m.user_id
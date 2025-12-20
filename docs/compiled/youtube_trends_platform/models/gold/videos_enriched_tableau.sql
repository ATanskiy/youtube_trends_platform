

SELECT
    video_id,
    region_id,
    language_id,
    language_id_src,
    channel_id,
    category_id,

    video_snapshot_at,
    video_published_at,

    CAST(video_snapshot_at AS DATE) AS video_snapshot_at_d,
    date_format(video_snapshot_at, 'HH:mm:ss') AS video_snapshot_at_t,

    year(video_snapshot_at)    AS year,
    quarter(video_snapshot_at) AS quarter,
    month(video_snapshot_at)   AS month,
    day(video_snapshot_at)     AS day,
    hour(video_snapshot_at)    AS hour,
    minute(video_snapshot_at)  AS minute,
    second(video_snapshot_at)  AS second,

    dayofweek(video_snapshot_at) AS dow,
    substr(date_format(video_snapshot_at, 'EEE'), 1, 1) AS dow_one_letter,
    date_format(video_snapshot_at, 'EEE')  AS dow_short,
    date_format(video_snapshot_at, 'EEEE') AS dow_full,

    dayofyear(video_snapshot_at)  AS doy,
    weekofyear(video_snapshot_at) AS week,

    view_count,
    like_count,
    favorite_count,
    comment_count,

    view_growth,
    like_growth,
    favorite_growth,
    comment_growth,

    title,
    description,
    duration,

    channel_title,
    category_name,
    language_name,
    region_name,

    latitude,
    longitude

FROM gold.videos_enriched
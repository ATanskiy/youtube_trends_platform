

SELECT *
FROM youtube_trends.gold.videos_enriched_tableau


WHERE video_snapshot_at >
    (SELECT max(video_snapshot_at) FROM "pg_bi_db"."bi"."pg_videos_enriched_tableau")

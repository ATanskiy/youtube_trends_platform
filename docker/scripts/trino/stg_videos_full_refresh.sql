-- 1. Recreate empty staging table
DROP TABLE IF EXISTS pg_bi_db.bi.stg_videos_tableau;

CREATE TABLE pg_bi_db.bi.stg_videos_tableau AS
SELECT *
FROM youtube_trends.gold.videos_enriched_tableau
WHERE 1 = 0;

-- 2. Insert delta only
INSERT INTO pg_bi_db.bi.stg_videos_tableau
SELECT *
FROM youtube_trends.gold.videos_enriched_tableau s
WHERE s.video_snapshot_at >
    (
        SELECT COALESCE(MAX(video_snapshot_at), TIMESTAMP '1970-01-01')
        FROM pg_bi_db.bi.pg_videos_enriched_tableau
    );

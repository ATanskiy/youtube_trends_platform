
        
            delete from "pg_bi_db"."bi"."pg_videos_enriched_tableau"
            where exists (
                select 1
                from "pg_bi_db"."bi"."pg_videos_enriched_tableau__dbt_tmp"
                where
                
                    "pg_bi_db"."bi"."pg_videos_enriched_tableau".video_id = "pg_bi_db"."bi"."pg_videos_enriched_tableau__dbt_tmp".video_id
                    and 
                
                    "pg_bi_db"."bi"."pg_videos_enriched_tableau".region_id = "pg_bi_db"."bi"."pg_videos_enriched_tableau__dbt_tmp".region_id
                    and 
                
                    "pg_bi_db"."bi"."pg_videos_enriched_tableau".video_snapshot_at = "pg_bi_db"."bi"."pg_videos_enriched_tableau__dbt_tmp".video_snapshot_at
                    
                
                )
                
            ;
        
    

    insert into "pg_bi_db"."bi"."pg_videos_enriched_tableau" ("video_id", "region_id", "language_id", "language_id_src", "channel_id", "category_id", "video_snapshot_at", "video_published_at", "video_snapshot_at_d", "video_snapshot_at_t", "year", "quarter", "month", "day", "hour", "minute", "second", "dow", "dow_one_letter", "dow_short", "dow_full", "doy", "week", "view_count", "like_count", "favorite_count", "comment_count", "view_growth", "like_growth", "favorite_growth", "comment_growth", "title", "duration", "channel_title", "category_name", "language_name", "region_name", "latitude", "longitude")
    (
        select "video_id", "region_id", "language_id", "language_id_src", "channel_id", "category_id", "video_snapshot_at", "video_published_at", "video_snapshot_at_d", "video_snapshot_at_t", "year", "quarter", "month", "day", "hour", "minute", "second", "dow", "dow_one_letter", "dow_short", "dow_full", "doy", "week", "view_count", "like_count", "favorite_count", "comment_count", "view_growth", "like_growth", "favorite_growth", "comment_growth", "title", "duration", "channel_title", "category_name", "language_name", "region_name", "latitude", "longitude"
        from "pg_bi_db"."bi"."pg_videos_enriched_tableau__dbt_tmp"
    )

      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "bi"."bi"."pg_videos_enriched_tableau" as DBT_INTERNAL_DEST
        using "pg_videos_enriched_tableau__dbt_tmp111543117310" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.video_id = DBT_INTERNAL_DEST.video_id
                ) and (
                    DBT_INTERNAL_SOURCE.region_id = DBT_INTERNAL_DEST.region_id
                ) and (
                    DBT_INTERNAL_SOURCE.video_snapshot_at = DBT_INTERNAL_DEST.video_snapshot_at
                )

    
    when matched then update set
        "video_id" = DBT_INTERNAL_SOURCE."video_id","region_id" = DBT_INTERNAL_SOURCE."region_id","language_id" = DBT_INTERNAL_SOURCE."language_id","language_id_src" = DBT_INTERNAL_SOURCE."language_id_src","channel_id" = DBT_INTERNAL_SOURCE."channel_id","category_id" = DBT_INTERNAL_SOURCE."category_id","video_snapshot_at" = DBT_INTERNAL_SOURCE."video_snapshot_at","video_published_at" = DBT_INTERNAL_SOURCE."video_published_at","video_snapshot_at_d" = DBT_INTERNAL_SOURCE."video_snapshot_at_d","video_snapshot_at_t" = DBT_INTERNAL_SOURCE."video_snapshot_at_t","year" = DBT_INTERNAL_SOURCE."year","quarter" = DBT_INTERNAL_SOURCE."quarter","month" = DBT_INTERNAL_SOURCE."month","day" = DBT_INTERNAL_SOURCE."day","hour" = DBT_INTERNAL_SOURCE."hour","minute" = DBT_INTERNAL_SOURCE."minute","second" = DBT_INTERNAL_SOURCE."second","dow" = DBT_INTERNAL_SOURCE."dow","dow_one_letter" = DBT_INTERNAL_SOURCE."dow_one_letter","dow_short" = DBT_INTERNAL_SOURCE."dow_short","dow_full" = DBT_INTERNAL_SOURCE."dow_full","doy" = DBT_INTERNAL_SOURCE."doy","week" = DBT_INTERNAL_SOURCE."week","view_count" = DBT_INTERNAL_SOURCE."view_count","like_count" = DBT_INTERNAL_SOURCE."like_count","favorite_count" = DBT_INTERNAL_SOURCE."favorite_count","comment_count" = DBT_INTERNAL_SOURCE."comment_count","view_growth" = DBT_INTERNAL_SOURCE."view_growth","like_growth" = DBT_INTERNAL_SOURCE."like_growth","favorite_growth" = DBT_INTERNAL_SOURCE."favorite_growth","comment_growth" = DBT_INTERNAL_SOURCE."comment_growth","title" = DBT_INTERNAL_SOURCE."title","duration" = DBT_INTERNAL_SOURCE."duration","channel_title" = DBT_INTERNAL_SOURCE."channel_title","category_name" = DBT_INTERNAL_SOURCE."category_name","language_name" = DBT_INTERNAL_SOURCE."language_name","region_name" = DBT_INTERNAL_SOURCE."region_name","latitude" = DBT_INTERNAL_SOURCE."latitude","longitude" = DBT_INTERNAL_SOURCE."longitude"
    

    when not matched then insert
        ("video_id", "region_id", "language_id", "language_id_src", "channel_id", "category_id", "video_snapshot_at", "video_published_at", "video_snapshot_at_d", "video_snapshot_at_t", "year", "quarter", "month", "day", "hour", "minute", "second", "dow", "dow_one_letter", "dow_short", "dow_full", "doy", "week", "view_count", "like_count", "favorite_count", "comment_count", "view_growth", "like_growth", "favorite_growth", "comment_growth", "title", "duration", "channel_title", "category_name", "language_name", "region_name", "latitude", "longitude")
    values
        ("video_id", "region_id", "language_id", "language_id_src", "channel_id", "category_id", "video_snapshot_at", "video_published_at", "video_snapshot_at_d", "video_snapshot_at_t", "year", "quarter", "month", "day", "hour", "minute", "second", "dow", "dow_one_letter", "dow_short", "dow_full", "doy", "week", "view_count", "like_count", "favorite_count", "comment_count", "view_growth", "like_growth", "favorite_growth", "comment_growth", "title", "duration", "channel_title", "category_name", "language_name", "region_name", "latitude", "longitude")


  
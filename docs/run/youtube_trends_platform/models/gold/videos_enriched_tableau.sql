
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
              
              
          
      
  

  

  merge into gold.videos_enriched_tableau as DBT_INTERNAL_DEST
      using videos_enriched_tableau__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.video_id = DBT_INTERNAL_DEST.video_id
               and 
                  DBT_INTERNAL_SOURCE.region_id = DBT_INTERNAL_DEST.region_id
               and 
                  DBT_INTERNAL_SOURCE.video_snapshot_at = DBT_INTERNAL_DEST.video_snapshot_at
              

      when matched then update set
         * 

      when not matched then insert *

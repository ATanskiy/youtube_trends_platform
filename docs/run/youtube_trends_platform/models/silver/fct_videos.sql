
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
              
              
          
      
  

  

  merge into silver.fct_videos as DBT_INTERNAL_DEST
      using fct_videos__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.video_id = DBT_INTERNAL_DEST.video_id
               and 
                  DBT_INTERNAL_SOURCE.region_id = DBT_INTERNAL_DEST.region_id
               and 
                  DBT_INTERNAL_SOURCE.snapshot_at = DBT_INTERNAL_DEST.snapshot_at
              

      when matched then update set
         * 

      when not matched then insert *

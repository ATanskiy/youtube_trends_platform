
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.dim_videos as DBT_INTERNAL_DEST
      using dim_videos__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.video_id = DBT_INTERNAL_DEST.video_id
          

      when matched then update set
         * 

      when not matched then insert *

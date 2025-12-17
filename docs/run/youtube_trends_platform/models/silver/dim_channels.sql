
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.dim_channels as DBT_INTERNAL_DEST
      using dim_channels__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.channel_id = DBT_INTERNAL_DEST.channel_id
          

      when matched then update set
         * 

      when not matched then insert *

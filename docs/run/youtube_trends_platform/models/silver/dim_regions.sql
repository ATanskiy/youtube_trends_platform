
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.dim_regions as DBT_INTERNAL_DEST
      using dim_regions__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.region_id = DBT_INTERNAL_DEST.region_id
          

      when matched then update set
         * 

      when not matched then insert *


    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.dim_languages as DBT_INTERNAL_DEST
      using dim_languages__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.language_id = DBT_INTERNAL_DEST.language_id
          

      when matched then update set
         * 

      when not matched then insert *

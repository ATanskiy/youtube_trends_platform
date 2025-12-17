
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.dim_categories as DBT_INTERNAL_DEST
      using dim_categories__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.category_id = DBT_INTERNAL_DEST.category_id
          

      when matched then update set
         * 

      when not matched then insert *


{{
    config(
        materialized='temp_table_chunks'
    )
}}
select 
     mod(number,{{var('chunks')}}) as _chunk,
    * 
from {{ ref('seed_test_file_2') }}
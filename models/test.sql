
{{
    config(
        materialized='temp_table_chunks'
    )
}}
select 
    mod(number,{{var('chunks')}}) as _chunk,
    * 
from {{ ref('test_file_1') }}

{{
    config(
        materialized='temp_table_chunks'
    )
}}
select * from {{ ref('test_file_1') }}
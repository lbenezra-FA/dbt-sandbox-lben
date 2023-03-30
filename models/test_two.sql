
{{
    config(
        materialized='temp_table_chunks'
    )
}}
select * from {{ ref('seed_test_file_2') }}
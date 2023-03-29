
{{
    config(
        materialized='temp_table'
    )
}}
select * from {{ ref('test_file_1') }}
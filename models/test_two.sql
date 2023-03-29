
{{
    config(
        materialized='temp_table'
    )
}}
select * from {{ ref('seed_test_file_2') }}
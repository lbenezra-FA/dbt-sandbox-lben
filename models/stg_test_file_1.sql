with source as (
    select * from {{ ref('test_file_1') }}
),

final as (
    select
        number as id,
        cats as cat_name, 
        parent_id
    from source
)
select * from final 
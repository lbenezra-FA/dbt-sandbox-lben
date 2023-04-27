with 
cats as ( 
    select * from {{ ref('stg_test_file_1') }}
)



select
    id, 
    cat_name,
    parent_id, 

    max(cat_name) over (partition by id order by 
        cat_name rows BETWEEN UNBOUNDED following and UNBOUNDED 
        following) last_event_date
    --lead(cat_name) over (partition by parent_id = id order by parent_id) as paren_cat_name_2
from cats order by id

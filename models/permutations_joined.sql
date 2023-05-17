-- join the results of two chunked tables, finding every permutation 
--depends on: {{ref('test')}}
--depends on: {{ref('test_two')}}

{{ config(
    materialized="join_temp_table_chunks_temporarily",
    
)}}

{#
{%for chunked_temp in range(var('chunks'))%}
    {%-set first_model_name -%}test__dbt_tmp__{{chunked_temp}}{%-endset-%}
    
    {%for other_chunked_temp in range(var('chunks'))%}
        {%-set second_model_name -%}test_two__dbt_tmp__{{other_chunked_temp}}{%-endset-%}

        {%-set sql_query %}
        select 
            first_model.number,
            first_model.cats as first_cats,
            second_model.cats as second_cats

        from `{{target.project}}`.`{{target.schema}}`.`{{first_model_name}}` as first_model
        left join `{{target.project}}`.`{{target.schema}}`.`{{second_model_name}}` as second_model 
            on first_model.number = second_model.number
        {%endset-%}


        {%if not loop.last-%}UNION ALL {%-endif%}
    {%endfor%}
    
   {%if not loop.last-%}UNION ALL {%-endif%}
{%endfor%}
#}
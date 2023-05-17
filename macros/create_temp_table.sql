{% macro create_temp_table() %}
CREATE TEMP TABLE joined__dbt_tmp_(number int64, first_cats string, second_cats string)
{{ sql_query() }};
{%endmacro%}

{% macro sql_query( )%}
--call macro to generate chunked temp
select 
    first_model.number,
    first_model.cats as first_cats,
    second_model.cats as second_cats

from `{{target.project}}`.`{{target.schema}}`.`{{first_model_name()}}` as first_model
left join `{{target.project}}`.`{{target.schema}}`.`{{second_model_name()}}` as second_model 
    on first_model.number = second_model.number
    
{% endmacro %}

{% macro first_model_name()%}
{#{%for chunked_temp in range(var('chunks'))%}#}
{%-set first_model_name -%}test__dbt_tmp__1{%-endset-%}{{first_model_name}}
{#{%endfor%}#}
{%endmacro%}

{%macro second_model_name()%}
{#{%for other_chunked_temp in range(var('chunks'))%}#}
{%-set second_model_name -%}test_two__dbt_tmp__2{%-endset-%}{{second_model_name}}
{%endmacro%}


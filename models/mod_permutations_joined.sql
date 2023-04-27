-- join the results of two chunked tables, finding every permutation 
--depends on: {{ref('test')}}
--depends on: {{ref('test_two')}}

{%for chunked_temp in range(1,10)%}
    {%-set first_model_name -%}test__dbt_tmp__{{chunked_temp}}{%-endset-%}
    
    {%for other_chunked_temp in range(1,10)%}
        {%-set second_model_name -%}test_two__dbt_tmp__{{other_chunked_temp}}{%-endset-%}
        {%-set set_alias%}mod_permutations_{{chunked_temp}}_{{other_chunked_temp}}{%-endset-%}
       
        {{
            config(
                alias= set_alias
            )
        }}
       
            select 
                first_model.number,
                first_model.cats as first_cats,
                second_model.cats as second_cats

            from `{{target.project}}`.`{{target.schema}}`.`{{first_model_name}}` as first_model
            left join `{{target.project}}`.`{{target.schema}}`.`{{second_model_name}}` as second_model 
                on first_model.number = second_model.number

       

    {%endfor%}    
{%endfor%}

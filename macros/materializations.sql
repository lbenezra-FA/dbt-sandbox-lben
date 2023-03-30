
{# the goal here is to create a materialization that takes a big-ass table,
    breaks it apart using a nested for-loop by permutation values,
    creates a temp table for each of those values that can build simultaneously,
    and then unions all the temp tables. 

    champions_score_household_attribute_presence_reduced ---> household_attribute_presence_chunks --|
                                                                                                    |--> 
    champions_score_profile_mapping ------------------------> profile_mapping_chunks ---------------|

profile_attribute_presence_chunks --> champions_profile_presence_chunks --> 
champions_profile_projected_population_sizes_chunks --> unioned_potential_profile_population_sizes
#}

{% materialization temp_table_chunks, adapter='bigquery' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}


---------------------------------------------------------------
  -- build model

{%for temp_chunk in range(var('temp_chunk_start'),var('temp_chunk_end'))%}  
    {%- set new_suffix -%}__dbt_tmp__{{temp_chunk}}{%- endset -%}
    {%- set intermediate_relation -%}{{make_intermediate_relation(target_relation, suffix=new_suffix)}}{%- endset -%}
    
    {% set new_sql %}
     {{sql}} where number = {{temp_chunk}}
    {%endset%}

    {% call statement('main') -%}
        {{ get_create_table_as_sql(True, intermediate_relation, new_sql) }}
    {%- endcall %}

{% endfor %}

---------------------------------------------------------------


  {#{{ adapter.rename_relation(intermediate_relation, target_relation) }}#}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}


  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
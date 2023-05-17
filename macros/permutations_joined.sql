
 {%macro permutations_joined(first_model_name, second_model_name)%}
 
 select * from (
            
            select 
                first_model.number,
                first_model.cats as first_cats,
                second_model.cats as second_cats
            from `{{target.project}}`.`{{this.schema}}`.`{{first_model_name}}` as first_model
            left join `{{target.project}}`.`{{this.schema}}`.`{{second_model_name}}` as second_model
            using (number)  

        ) 
{%endmacro%}
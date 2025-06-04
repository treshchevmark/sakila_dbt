{{ 
    dbt_utils.union_relations(relations=[ref('dim_customer'), ref('dim_date') ])
}}
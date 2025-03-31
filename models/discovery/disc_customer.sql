/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Surrogate key assigment
  - Naming conventions alignment
*/

{#/*
  The surrogate key logic (lookup and generation) uses the macros provided in the surrogate_keys.sql file.
  The surrogate keys used by this model only need to be defined in the surrogate_keys dictionnary at the beginning.
  Note that the code below assumes the following conventions, which are not currently enforced in the models generating the key table:
  - Key table name: key_{{class}} (eg. key_customer)
  - Surrogate key column name in key table: {{class}}_key (eg. customer_key)
  - Natural key column name in key table: {{class}}_nk (eg. customer_nk)
  - We use the surrogate value -1 in all classes to signify "unknown".
*/#}


{# Define the surrogate keys here #}
{%-
  set surrogate_keys={
      'customer':{
        'source_table': 'raw_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['email'],
        'domain': 'retail',
      }
    }
-%}

{%- set surrogate_keys_hook = generate_surrogate_key_hook(surrogate_keys) -%}

{{ config(
    materialized='otf_materialize',
    datalake='aws_glue_catalog',
    datalake_database='sbx',
    incremental_strategy='delete+insert',
    unique_key='customer_key',
    pre_hook=surrogate_keys_hook
    ) 
}}

SELECT 
s.*
--Surrogate key columns
{%- for sk, params in surrogate_keys.items() %}
,coalesce({{sk}}.{{ params['key_table'].split('_', 1)[1] }}_key,-1) {{sk}}_key
{%- endfor %}
from {{ ref('raw_customers') }} s
--Surrogate key joins
{%- for sk, params in surrogate_keys.items() %}
left join {{ref(params['key_table'])}} {{sk}}
  on {{sk}}.{{ params['key_table'].split('_', 1)[1] }}_nk={{generate_natural_key(params['natural_key_cols'])}}
  and {{sk}}.domain_cd='{{params['domain']}}'
{% endfor %}
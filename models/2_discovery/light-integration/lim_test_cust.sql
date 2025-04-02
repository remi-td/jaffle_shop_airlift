{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
  )
}}

select * 
from aws_glue_catalog.{{ ref('raw_customers') }}
/*
  Build a light discovery layer from raw data
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Surrogate key assigment
  - Naming conventions alignment
*/


{{ config(
    materialized='otf_materialize',
    datalake=var('otf_datalake'),
    datalake_database=var('otf_database'),
    incremental_strategy='delete+insert',
    unique_key='product_cd',
    ) 
}}


{#-
In most cases we want to perform a 1:1 projection from source image to lightly integrated, and simply add keys
however we may have to pre-join some tables, mask some columns, apply naming convention
or perform complex transformation logic to derrive natural keys
do this here.
#}

select
product_code product_cd
,current_timestamp order_update_dttm
,product_name
,unit_price
,start_date
,end_date
from {{ ref('raw_product_catalog') }} s
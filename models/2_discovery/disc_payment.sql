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
    unique_key='order_key',
    ) 
}}


{#-
In most cases we want to perform a 1:1 projection from source image to lightly integrated, and simply add keys
however we may have to pre-join some tables, mask some columns, apply naming convention
or perform complex transformation logic to derrive natural keys
do this here.
#}

select
s.id payment_key
,current_timestamp payment_update_dttm
,s.payment_amount
,customer.customer_key
,s.order_id order_key
,s.payment_tstmp payment_dttm
from {{ ref('raw_payments') }} s
--Get the customer key from the related customers table 
--this will create a dependency, so this model runs after the disc_customers table is populated
left join {{ var('otf_datalake') }}.{{ref('disc_customer')}} customer
  on customer.id=s.customer_id
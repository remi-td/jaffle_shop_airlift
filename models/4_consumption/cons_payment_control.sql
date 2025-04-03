{{ config(
    materialized='otf_materialize',
    datalake=var('otf_datalake'),
    datalake_database=var('otf_database'),
    incremental_strategy='delete+insert',
    unique_key='source_system_payment_key'
) 
}}

select 
p.payment_dttm
,o.order_dttm
,p.payment_amount
,o.checkout_sum order_checkout_sum
,o.order_status
,p.payment_key source_system_payment_key
from {{ var('otf_datalake') }}.{{ ref('disc_payment') }} p
left join {{ var('otf_datalake') }}.{{ ref('disc_order') }} o
    on o.order_key = p.order_key
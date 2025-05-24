{{ config(
    materialized='otf_materialize',
    datalake=var('otf_datalake'),
    datalake_database=var('otf_database'),
    incremental_strategy='delete+insert',
    unique_key='source_system_payment_id'
) 
}}

select 
p.payment_tstmp payment_dttm
,o.order_tstmp order_dttm
,p.payment_amount
,o.checkout_sum order_checkout_sum
,o.order_status
,p.id source_system_payment_id
,c.email
from al210ghxMjTZ.disc_payments p
left join al210ghxMjTZ.disc_orders o
    on o.id = p.order_id
left join al210ghxMjTZ.disc_customers c
    on o.customer_id = c.customer_key
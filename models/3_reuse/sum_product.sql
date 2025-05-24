{{
  config(
    materialized='incremental',
    unique_key='product_cd',
    incremental_strategy='valid_history',
    valid_period='valid_period'

  )
}}


{#
This model defines the product summary.
Here we compute common key metrics at the product level.
The results history will be retained on a real-world timeline to allow historical analysis,
the valid time period calculation is implemented by the materialisation strategy (see config() above).
#}

select
    --the history logic expects LATIN ... this will need to be fixed
    translate(cast(p.product_code as VARCHAR(10) NOT CASESPECIFIC) USING UNICODE_TO_LATIN) product_cd
    ,translate(cast(p.product_name as VARCHAR(200) NOT CASESPECIFIC) USING UNICODE_TO_LATIN) product_name
    -- The mesure is valid as of the latest order time and until changed
    ,period(max(order_tstmp), ('9999-12-31 23:59:59.999999' (timestamp))) valid_period
    ,sum(o.quantity) product_order_cnt
    ,sum(o.checkout_sum) product_order_amt
from "al210ghxMjTZ"."disc_product_catalog" p
left join "al210ghxMjTZ"."disc_orders" o
    on o.product_code = p.product_code
group by 1, 2

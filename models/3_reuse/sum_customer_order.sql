{{
  config(
    materialized='incremental',
    unique_key=['customer_key', 'product_cd'],
    incremental_strategy='valid_history',
    valid_period='valid_period'

  )
}}


{#
This model defines the orders summary.
Here we compute common order metrics at the customer x product level.
The results history will be retained on a real-world timeline to allow historical analysis,
the valid time period calculation is implemented by the materialisation strategy (see config() above).
#}

select
    o.customer_key
    ,o.product_cd
    -- The mesure is valid as of the latest order time and until changed
    ,period(max(order_dttm), ('9999-12-31 23:59:59.999999' (timestamp))) valid_period
    ,sum(distinct o.quantity) customer_order_cnt
    ,sum(o.checkout_sum) customer_order_amt
from {{ var('otf_datalake') }}.{{ ref('disc_order') }} o
group by 1, 2

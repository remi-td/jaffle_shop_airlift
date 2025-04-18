with customer_favourite_products as 
(
    --Let's gather our customer favourite products. 
    --Since this will be consumed by a customer interaction agent, a short list will do
    sel
    customer_key
    ,trim( trailing ',' from (XMLAGG( product_name ||',' order by product_order_cnt ) (VARCHAR(1000)))) top_ordered_products
    from 
    (
        sel customer_key
        ,product_cd
        ,rank() over(partition by customer_key order by customer_order_cnt) prod_rank
        from {{ref('sum_customer_order')}} 
        where valid_period contains current_timestamp
        qualify prod_rank <=3
    ) o
    join {{ref('sum_product')}} p
        on o.product_cd=p.product_cd
        and p.valid_period contains current_timestamp
    group by 1
)

, everyonelse_favourite_products as 
(
    --Let's gather the overall favourite products that were NOT tried yet by each customer
    --Since this will be consumed by a customer interaction agent, a short list will do
    sel
    customer_key
    ,trim( trailing ',' from (XMLAGG( product_name ||',' order by product_order_cnt ) (VARCHAR(1000)))) top_ordered_products
    from 
    (
        sel customer_key
        ,product_cd
        ,product_name
        ,product_order_cnt
        ,rank() over(partition by customer_key order by product_order_cnt) prod_rank
        --all product x customer combinations
        from {{ref('sum_product')}} p
        cross join {{ref('ref_customer')}} c
        where p.valid_period contains current_timestamp
        --...that have not yet been seen in orders
        and not exists
        (sel 1 from {{ref('sum_customer_order')}} o where o.product_cd=p.product_cd and  c.customer_key=o.customer_key)
        qualify prod_rank <=3
    ) p
    group by 1    
)

,customer_timeline as
(
    sel
    customer_key
    ,INTERVAL(PERIOD(last_order, current_timestamp)) DAY(4) last_order_days_ago
    ,INTERVAL(PERIOD(first_order, last_order)) MONTH customer_for_month
    from
    (
        sel customer_key
        customer_key
        ,max(begin(valid_period)) last_order
        ,min(begin(valid_period)) first_order
        from {{ref('sum_customer_order')}} 
        group by 1
    ) a
)

,customer_value as
(
    sel 
    customer_key
    ,sum(customer_order_amt) customer_value_amt
    ,percent_rank() over(order by customer_value_amt) value_rank
    from {{ref('sum_customer_order')}} 
    group by 1
    where valid_period contains current_timestamp
)

,final as (

    select
        customer.customer_key,
        'Last ordered '||trim(customer_timeline.last_order_days_ago (int))||' days ago' last_order,
        'Has been customer for '||trim(customer_timeline.customer_for_month  (int))||' months' customer_since,
        'Customer value is '||trim(round(customer_value.value_rank*10,1) (decimal(3,1)))|| '/ 10' as customer_value,
        'Customer favourite products '||customer_favourite_products.top_ordered_products customer_favourite_products,
        'Customer should try '||everyonelse_favourite_products.top_ordered_products everyonelse_favourite_products

    from {{ref('ref_customer')}} customer
    left join  customer_timeline on customer_timeline.customer_key=customer.customer_key
    left join  customer_favourite_products on customer_favourite_products.customer_key=customer.customer_key
    left join  everyonelse_favourite_products on everyonelse_favourite_products.customer_key=customer.customer_key
    left join  customer_value on customer_value.customer_key=customer.customer_key
)

select * from final
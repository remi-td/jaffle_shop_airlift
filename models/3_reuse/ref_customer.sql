{#
This model defines the Customer major entity.
This is a simple projection from the key table, 
technically we could use the key table, but it may contain sensitive information,
and this makes the model on this layer coherent for its users.
#}

locking row for access
select
    customer_key
    ,REGEXP_REPLACE(customer_nk, '^(.{3})[^@]*(@.*)$', '\1***\2', 1, 1, 'i') AS email --partially mask the email
from {{ ref('key_customer') }}
where domain_cd = 'retail'

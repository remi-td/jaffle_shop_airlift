select
spend,
nearby5,
nearby10,
new ST_GEOMETRY(ptLocWkt) location_pt
from {{ ref('stg_market_customers') }} s
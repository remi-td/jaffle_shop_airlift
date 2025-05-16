/*
Build a light discovery layer from raw data for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Surrogate key assigment
  - Naming conventions alignment
*/


SELECT 
*
from {{ ref('raw_customers') }} s

{#-/*
Generic source image build from staging table, two blocks:
1. We use a common macro build_source_image() that
  "mirrors" the structure of the source table, 
  accounting for history preparation and column filtering, if specified.

Model Usage:
1. Update the config parameters (all optional):
    - exclude_columns: List of columns to exclude, if any
    - If needed: materialized and incremental_strategy
    - If materialized='incremental' and incremental_strategy in ('delete+insert', 'merge', 'valid_history')
      - unique_key: Primary key in the source table (temporal column excluded) 
    - If materialized='incremental' and incremental_strategy = 'valid_history'
      - valid_from: Name of the column indicating the record change date or time in the source system (If None and this is , then use current timestamp)
      - valid_period: Name of the column indicating the record valid period in the model
    - disable_delta: Set in order to disable the delta logic.

2. Update source_table (name of the staging table we build from) parameter in the macro, the others are infered from the configuration

Possible further customization: 
  - infer the source table name from the current model name (by simply changing the suffix) in the  build_source_image macro.
  - test the use on_schema_change='sync_all_columns'
*/-#}

{{
  config(
    unique_key=['id'],
    materialized='incremental',
    incremental_strategy='delete+insert'
  )
}}


with prepared_source_image as ( 
  {{ build_source_image (source_table='stg_orders', config=config) }}
)
select * from prepared_source_image
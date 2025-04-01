{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set datalake = node.config.datalake -%}
    {%- set datalake_database = node.config.datalake_database -%}
    {{ log("Debugging: DataLake: " ~ datalake, info=True) }}
    {{ log("Debugging: database: " ~ datalake_database, info=True) }}
    {{ log("Debugging: custom_schema_name: " ~ custom_schema_name, info=True) }}
    {%- if datalake and datalake_database  -%}
        {{ datalake_database }}
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim | lower }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
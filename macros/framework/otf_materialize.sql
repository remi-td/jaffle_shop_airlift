{% macro teradata__get_relation_otf(datalake, database, identifier) %}
    -- Custom logic to interface with Iceberg metadata
    {% set sql %}
        HELP DATABASE {{ datalake }}.{{ database }}
    {% endset %}
    {% set result = run_query(sql) %}
    {% if execute %}
        {% set table_names = result.columns[0].values() %}
        {{ log("Debugging: Tables in OTF database: " ~ result.columns[0].values(), info=True) }}
        {% if identifier in table_names %}
            {{ log("Debugging: Table already exists", info=True) }}
            {% do return(api.Relation.create(
                database=datalake,
                schema=database,
                identifier=identifier,
                type='table'
            )) %}
        {% else %}
            {% do return(None) %}
        {% endif %}
    {% else %}
        {% do return(None) %}
    {% endif %}
{% endmacro %}

{% materialization otf_materialize, adapter='teradata' %}

  {%- set datalake = config.get('datalake') -%}
  {%- set database = config.get('datalake_database') -%}

  {% set target_relation = teradata__get_relation_otf(
    datalake=datalake,
    database=database,
    identifier=this.identifier
  ) %}

  {% set exists = target_relation is not none %}
  {{ log("Debugging: Target exists: " ~ exists, info=True) }}
  {{ log("Debugging: Target relation: " ~ target_relation, info=True) }}  
  {% if target_relation is not none %}
    {{ log("Debugging: Drop OTF table: " ~ exists, info=True) }}
    {% call statement('drop_otf_target') %}
    DROP TABLE {{ datalake }}.{{ database }}.{{ this.identifier }} PURGE ALL;;
    {% endcall %}
  {% else %}

    {% set target_relation = api.Relation.create(
                database=datalake,
                schema=database,
                identifier=this.identifier,
                type='table') 
    %}

  {% endif %}

  {% if adapter.get_relation(this.database, this.schema, identifier='__tmp_' ~ this.identifier)  is not none %}
    {% call statement('pre_cleanup') %}
        DROP TABLE {{ this.schema }}.__tmp_{{ this.identifier }}
    {% endcall %}
  {% endif %}

  {% call statement('pre') %}
    CREATE TABLE {{ this.schema }}.__tmp_{{ this.identifier }}
    AS (
      {{ sql }}
    )
    WITH DATA;
  {% endcall %}

  {% call statement('main') %}
    CREATE TABLE {{ datalake }}.{{ database }}.{{ this.identifier }} AS {{ this.schema }}.__tmp_{{ this.identifier }} WITH DATA;
  {% endcall %}

  {% call statement('post') %}
    DROP TABLE __tmp_{{ this.identifier }};
  {% endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
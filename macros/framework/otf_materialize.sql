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

  {#-- Retrieve custom configurations --#}
  {%- set datalake = config.get('datalake') -%}
  {%- set database = config.get('datalake_database') -%}

  {#-- Execute pre-hooks defined on the model --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Check for an existing target table using custom logic --#}
  {% set target_relation = teradata__get_relation_otf(
      datalake=datalake,
      database=database,
      identifier=this.identifier
    ) %}
  {% set exists = target_relation is not none %}
  {{ log("Debugging: Target exists: " ~ exists, info=True) }}
  {{ log("Debugging: Target relation: " ~ target_relation, info=True) }}

  {#-- If the table exists, drop it; otherwise create a new relation object --#}
  {% if target_relation is not none %}
    {{ log("Debugging: Dropping existing OTF table", info=True) }}
    {% call statement('drop_otf_target') %}
      DROP TABLE {{ datalake }}.{{ database }}.{{ this.identifier }} PURGE ALL;
    {% endcall %}
  {% else %}
    {% set target_relation = api.Relation.create(
      database=datalake,
      schema=database,
      identifier=this.identifier,
      type='table'
    ) %}
  {% endif %}

  {#-- Optional pre-cleanup: Drop any temporary table left from a previous run --#}
  {% if execute %}
    {% set check_sql %}
      SEL COUNT(1) FROM dbc.tablesV WHERE tablename='__tmp_{{ this.identifier }}' AND databasename=user
    {% endset %}
    {% set result = run_query(check_sql) %}
    {% if result and result.columns[0].values()[0] | int > 0 %}
      {% call statement('pre_cleanup') %}
        DROP TABLE __tmp_{{ this.identifier }};
      {% endcall %}
    {% endif %}
  {% endif %}

  {#-- Create a temporary table for staging model output --#}
  {% call statement('pre') %}
    CREATE TABLE __tmp_{{ this.identifier }} AS (
      {{ sql }}
    ) WITH DATA;
  {% endcall %}

  {#-- Create the final target table from the temporary table --#}
  {% call statement('main') %}
    CREATE TABLE {{ datalake }}.{{ database }}.{{ this.identifier }} AS __tmp_{{ this.identifier }} WITH DATA;
  {% endcall %}

  {#-- Drop the temporary table after successful creation --#}
  {% call statement('post') %}
    DROP TABLE __tmp_{{ this.identifier }};
  {% endcall %}

  {#-- Execute post-hooks defined on the model --#}
  {{ run_hooks(post_hooks) }}

  {#-- Persist model documentation into your warehouse metadata --#}
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
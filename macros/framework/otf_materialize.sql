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
  {%- set target_db = config.get('datalake_database') -%}
  {%- if not target_db -%}
    {%- set target_db = this.schema -%}
  {%- endif -%}

  {#-- If scratch_database is defined in the variables, we will use it to store the temporary data, 
    otherwise use the current database --#}
  {%- set scratch_database = var('scratch_database', default=None) -%}
  {% if not scratch_database %}
    {% set current_db_query = "select database" %}
    {% set current_db_result = run_query(current_db_query) %}
    {% if execute and current_db_result and current_db_result.columns[0].values()|length > 0 %}
        {% set scratch_database = current_db_result.columns[0].values()[0] %}
    {% endif %}
  {% endif %}


  {#-- If we don't have a scratch_database value and the current database couldn't be identified,
       the temporary table name won't be fully qualified.
   --#}
  {% set scratch_prefix = scratch_database ~ '.' if scratch_database else "" %}

  {#-- Execute pre-hooks defined on the model --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Check for an existing target table using custom logic --#}
  {% set target_relation = teradata__get_relation_otf(
      datalake=datalake,
      database=target_db,
      identifier=this.identifier
    ) %}
  {% set exists = target_relation is not none %}
  {{ log("Debugging: Target exists: " ~ exists, info=True) }}
  {{ log("Debugging: Target relation: " ~ target_relation, info=True) }}

  {#-- If the table exists, drop it; otherwise create a new relation object --#}
  {% if target_relation is not none %}
    {{ log("Debugging: Dropping existing OTF table", info=True) }}
    {% call statement('drop_otf_target') %}
      DROP TABLE {{ datalake }}.{{ target_db }}.{{ this.identifier }} PURGE ALL;
    {% endcall %}
  {% else %}
    {% set target_relation = api.Relation.create(
      database=datalake,
      schema=target_db,
      identifier=this.identifier,
      type='table'
    ) %}
  {% endif %}

  {#-- Optional pre-cleanup: Drop any temporary table from a previous run in the scratch area --#}
  {% if execute %}
    {% set check_sql %}
      SEL count(1) FROM dbc.tablesV 
      WHERE lower(tablename)=lower('__tmp_{{ this.identifier }}')
      AND lower(databasename)=lower({% if scratch_database %}'{{ scratch_database }}'{% else %}database{% endif %})
    {% endset %}
    {% set result = run_query(check_sql) %}

    {% if result and result.columns[0].values()[0] | int > 0 %}
      {% call statement('pre_cleanup') %}
        DROP TABLE {{ scratch_prefix }}__tmp_{{ this.identifier }};
      {% endcall %}
    {% endif %}
  {% endif %}

  {#-- Create a temporary table in the designated scratch location for staging model output --#}
  {% call statement('pre') %}
    CREATE TABLE {{ scratch_prefix }}__tmp_{{ this.identifier }} AS (
      {{ sql }}
    ) WITH DATA;
  {% endcall %}

  {#-- Create the final target table from the temporary table --#}
  {% call statement('main') %}
    CREATE TABLE {{ datalake }}.{{ target_db }}.{{ this.identifier }} AS 
      {{ scratch_prefix }}__tmp_{{ this.identifier }}
    WITH DATA;
  {% endcall %}

  {#-- Drop the temporary table after successful creation --#}
  {% call statement('post') %}
    DROP TABLE {{ scratch_prefix }}__tmp_{{ this.identifier }};
  {% endcall %}

  {#-- Execute post-hooks defined on the model --#}
  {{ run_hooks(post_hooks) }}

  {#-- Persist model documentation into the warehouse metadata --#}
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
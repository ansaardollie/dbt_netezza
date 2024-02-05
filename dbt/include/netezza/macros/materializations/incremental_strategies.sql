-- Override to modify behavior for multiple unique keys
-- Source: https://github.com/dbt-msft/dbt-sqlserver/blob/v1.4.latest/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            where exists (
                select null
                from {{ source }}
                where
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}

            )
            {% if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {% endif %};
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};
        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
{% endmacro %}

-- Adds semicolons as noted here: https://github.com/dbt-msft/dbt-sqlserver/blob/master/dbt/include/sqlserver/macros/materializations/models/incremental/merge.sql
{% macro netezza__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}

{% macro netezza__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}
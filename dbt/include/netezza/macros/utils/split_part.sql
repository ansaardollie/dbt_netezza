{% macro netezza__split_part(string_text, delimiter_text, part_number) %}
    {% if part_number >= 0 %}
        get_value_varchar(array_split({{string_text}}, {{delimiter_text}}), {{part_number}})
    {% else %}
        get_value_varchar(array_split({{string_text}}, {{delimiter_text}}), array_count(array_split({{string_text}}, {{delimiter_text}})) + 1 {{part_number}})
    {% endif %}
{% endmacro %}
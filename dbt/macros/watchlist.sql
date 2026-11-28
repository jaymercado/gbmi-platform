{#-
    The size of the watchlist of the stack: the variable watchlist_size, or the setting
    GBMI_WATCHLIST_SIZE which the SEC pipelines read too, or 50 (week 3 plan 3.3). The demo
    stack follows the five companies of the recorded files.
-#}
{% macro watchlist_size() -%}
    {{ var('watchlist_size', env_var('GBMI_WATCHLIST_SIZE', '50')) | int }}
{%- endmacro %}

{% macro number_range(number) %}
    CASE 
        WHEN {{ number }} IS NULL OR {{ number }} = 0 THEN NULL
        WHEN {{ number }} <= 10 THEN '1-10'
        WHEN {{ number }} <= 50 THEN '11-50'
        WHEN {{ number }} <= 200 THEN '51-200'
        WHEN {{ number }} <= 500 THEN '201-500'
        WHEN {{ number }} <= 1000 THEN '501-1000'
        WHEN {{ number }} <= 5000 THEN '1001-5000'
        WHEN {{ number }} <= 10000 THEN '5001-10000'
        ELSE '10000+'
    END
{% endmacro %} 
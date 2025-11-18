{% macro html_to_markdown(text_value) %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                {{ text_value }},
                                -- Convert headers (h1-h6)
                                '<h1[^>]*>([^<]*)</h1>', '\n# \1\n'
                            ),
                            '<h2[^>]*>([^<]*)</h2>', '\n## \1\n'
                        ),
                        -- Convert line breaks and paragraphs
                        '<br[^>]*>|</p>|<p[^>]*>', 
                        '\n\n'
                    ),
                    -- Convert bold
                    '<(strong|b)>([^<]*)</[^>]*>', 
                    '**\2**'
                ),
                -- Convert italic
                '<(em|i)>([^<]*)</[^>]*>', 
                '_\2_'
            ),
            -- Convert lists
            '<li[^>]*>([^<]*)</li>',
            '\n- \1'
        ),
        -- Remove any remaining HTML tags and entities
        '<[^>]+>|&[a-z]+;|&#[0-9]+;',
        ' '
    )
{% endmacro %} 
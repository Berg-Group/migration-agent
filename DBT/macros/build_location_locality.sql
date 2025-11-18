{% macro build_location_locality(street1, street2, city, state, zip, country) %}
    CASE 
        WHEN COALESCE(NULLIF({{ street1 }}, ''), NULLIF({{ street2 }}, ''), NULLIF({{ city }}, ''), NULLIF({{ state }}, ''), NULLIF({{ zip }}, ''), NULLIF({{ country }}, '')) IS NULL 
        THEN NULL
        ELSE btrim(regexp_replace(
            COALESCE(NULLIF({{ street1 }}, ''), '') ||
            CASE 
                WHEN NULLIF({{ street2 }}, '') IS NOT NULL AND NULLIF({{ street1 }}, '') IS NOT NULL 
                THEN ', ' || {{ street2 }}
                WHEN NULLIF({{ street2 }}, '') IS NOT NULL 
                THEN {{ street2 }}
                ELSE ''
            END ||
            CASE 
                WHEN NULLIF({{ city }}, '') IS NOT NULL AND (
                    NULLIF({{ street1 }}, '') IS NOT NULL OR 
                    NULLIF({{ street2 }}, '') IS NOT NULL
                )
                THEN ', ' || {{ city }}
                WHEN NULLIF({{ city }}, '') IS NOT NULL 
                THEN {{ city }}
                ELSE ''
            END ||
            CASE 
                WHEN NULLIF({{ state }}, '') IS NOT NULL AND (
                    NULLIF({{ street1 }}, '') IS NOT NULL OR 
                    NULLIF({{ street2 }}, '') IS NOT NULL OR 
                    NULLIF({{ city }}, '') IS NOT NULL
                )
                THEN ', ' || {{ state }}
                WHEN NULLIF({{ state }}, '') IS NOT NULL 
                THEN {{ state }}
                ELSE ''
            END ||
            CASE 
                WHEN NULLIF({{ zip }}, '') IS NOT NULL AND (
                    NULLIF({{ street1 }}, '') IS NOT NULL OR 
                    NULLIF({{ street2 }}, '') IS NOT NULL OR 
                    NULLIF({{ city }}, '') IS NOT NULL OR 
                    NULLIF({{ state }}, '') IS NOT NULL
                )
                THEN ', ' || {{ zip }}
                WHEN NULLIF({{ zip }}, '') IS NOT NULL 
                THEN {{ zip }}
                ELSE ''
            END ||
            CASE 
                WHEN NULLIF({{ country }}, '') IS NOT NULL AND (
                    NULLIF({{ street1 }}, '') IS NOT NULL OR 
                    NULLIF({{ street2 }}, '') IS NOT NULL OR 
                    NULLIF({{ city }}, '') IS NOT NULL OR 
                    NULLIF({{ state }}, '') IS NOT NULL OR 
                    NULLIF({{ zip }}, '') IS NOT NULL
                )
                THEN ', ' || {{ country }}
                WHEN NULLIF({{ country }}, '') IS NOT NULL 
                THEN {{ country }}
                ELSE ''
            END,
            '[^a-zA-Z0-9, ]+',
            ' '
        ))
    END
{% endmacro %} 
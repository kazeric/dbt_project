{% macro month_assignment(date_column) %}

    case 
        when extract(day from {{ date_column }}) >= 26 then 
            date_trunc('month', dateadd(month, 1, {{ date_column }}))
        else 
            date_trunc('month', {{ date_column }})
    end
     
{% endmacro %}

-- this is the macro to truncare the data to the month using the 26 rule
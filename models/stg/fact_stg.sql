
{{ config(
    materialized='incremental',
    unique_key='activity_id',
    incremental_strategy='delete+insert',
    cluster_by=['activity_date', 'chv_id']
) }}

WITH FACT_STAGE AS (
    SELECT
        activity_id ,
        chv_id ,
        activity_date ,
        activity_timestamp ,
        activity_type ,
        household_id ,
        patient_id ,
        location_id ,
        is_deleted     ,
        created_at ,
        updated_at   

    FROM marts.fct_chv_activity
    WHERE activity_date IS NOT NULL
        AND chv_id IS NOT NULL
        AND is_deleted = FALSE

    {% if is_incremental() %}
          AND activity_date >= (SELECT COALESCE(MAX(activity_date), '1900-01-01') - interval '1 month' FROM {{ this }})
    {% endif %}
)

SELECT * FROM FACT_STAGE
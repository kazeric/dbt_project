/*
Model: chw_activity_monthly
Description: Monthly aggregation of CHW activities for dashboard performance metrics

TODO: Complete this dbt model to aggregate CHW activities by month

Instructions:
1. Add the dbt config block (materialization, unique_key, incremental_strategy)
2. Filter out invalid records (NULL chv_id, NULL activity_date, deleted records)
3. Use the month_assignment macro to calculate report_month
4. Aggregate metrics: total_activities, unique_households_visited, unique_patients_served, pregnancy_visits, child_assessments, family_planning_visits
5. GROUP BY chv_id and report_month
6. Add incremental logic 
*/

-- ============================================
-- TODO: Add dbt config block here
-- Required: materialized, unique_key, incremental_strategy
-- See business_requirements.md for materialization requirements
-- ============================================


-- ============================================
-- Main Query
-- ============================================


{{ config(
    materialized='incremental',
    unique_key=['chv_id', 'report_month'],
    incremental_strategy='delete+insert',
    schema = 'MARTS'
) }}

-- CTE to get the source sata
with source_data as (

    select
        activity_id,
        chv_id,
        activity_date,
        activity_type,
        household_id,
        patient_id,
        is_deleted,
        created_at,
        updated_at
    from {{ ref('fact_stg') }}

    where chv_id is not NULL
    and activity_date is not NULL
    and is_deleted = FALSE


),

report_month_cte as (

    select
        *,
        {{month_assignment('activity_date')}} as report_month
    from source_data

),

aggregated as (

    select
        chv_id,
        report_month,
        count(activity_id) as total_activities,
        count(distinct household_id) as unique_households_visited,
        count(distinct patient_id) as unique_patients_served,
        sum(case when activity_type = 'pregnancy_visit' then 1 else 0 end ) as pregnancy_visits,
        sum(case when activity_type = 'child_assessment' then 1 else 0 end) as child_assessments,
        sum(case when activity_type = 'family_planning' then 1 else 0 end) as family_planning_visits


    from report_month_cte
    group by chv_id, report_month


)

select * from aggregated
    {% if is_incremental() %}
        where report_month >= dateadd(month, -3, current_date)  
    {% endif %}

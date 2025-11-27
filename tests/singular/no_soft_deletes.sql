SELECT 
    activity_id
FROM 
    {{ ref('fact_stg') }}
WHERE is_deleted = true

-- this is a test to ensure that soft deletes dont appear in the staged data 
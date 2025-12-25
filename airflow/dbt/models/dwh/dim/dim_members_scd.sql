{{ config(
    materialized='incremental',
    unique_key='member_sk'
) }}

SELECT
   {# {{ dbt_utils.generate_surrogate_key([
        'user_id',
        'dbt_valid_from'
    ]) }} AS member_sk,#}

    user_id AS member_id,
    user_name,
    company_email AS member_email,
    staff_code,
    branch_id,
    department_id,
    position_id,
    user_level,
    user_status,

    dbt_valid_from::DATE AS effective_from,
    COALESCE(dbt_valid_to::DATE, DATE '2999-12-31') AS effective_to,
    dbt_valid_to IS NULL AS is_current

FROM {{ ref('members_snapshot') }}
WHERE company_email IS NOT NULL

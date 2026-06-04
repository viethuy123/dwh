{% snapshot odoo_members_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='member_id',

        strategy='check',

        check_cols=[
            'company_id',
            'division_id',
            'job_id',
            'parent_id',
            'coach_id',
            'contract_id',
            'type_member_id',
            'level',
            'rank_id',
            'job_title',
            'marital',
            'state',
            'is_active',
            'issue_date_identification'
        ],

        invalidate_hard_deletes=True
    )
}}

SELECT

    -- Keys
    member_id,
    member_code,

    -- Basic Information
    name,
    job_title,
    gender,
    marital,

    -- Contact
    work_email,
    work_phone,
    mobile_phone,

    -- Employment Dates
    birthday,
    issue_date_identification,
    joining_date,
    start_working_date,
    probation_start_date,
    traineeship_start_date,
    resign_date,
    departure_date,

    -- Organization
    company_id,
    division_id,
    job_id,
    parent_id,
    coach_id,

    -- Employee Structure
    contract_id,
    type_member_id,
    level,
    rank_id,

    -- Status
    state,
    state_root,
    is_active,

    -- Useful attributes
    member_type,
    resource_calendar_id,
    academic_level_id,
    qualification_id,

    -- Audit
    create_date,
    write_date,
    etl_datetime

FROM {{ ref('odoo_hr_member') }}

{% endsnapshot %}

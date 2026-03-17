{% snapshot members_snapshot_full %}

{{
    config(
      unique_key='user_id',
      post_hook=[
              "CREATE INDEX IF NOT EXISTS idx_email ON {{ this }} (company_email)"
            ],
      strategy='timestamp',
      updated_at='update_time',
      invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    user_name,
    is_deleted,
    company_email,
    staff_code,
    branch_id,
    department_id,
    position_id,
    user_level,
    user_status,
    create_time,
    update_time,
    expired_time,
    welcome_day,
    job_id,
    performance_factor,
    sub_position_id,
    etl_datetime


FROM {{ ref('users') }}
{% endsnapshot %}
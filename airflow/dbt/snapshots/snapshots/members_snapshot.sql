{% snapshot members_snapshot %}

{{
    config(
      unique_key='user_id',
      post_hook=[
              "CREATE INDEX IF NOT EXISTS idx_email ON {{ this }} (email)"
            ],
      strategy='check',
      check_cols=[
      'user_status',
      'branch_id',
      'department_id',
      'position_id',
      'user_level'
    ],
      invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    user_name,
    company_email,
    staff_code,
    branch_id,
    department_id,
    position_id,
    user_level,
    user_status,
    create_time,
    update_time,
    expired_time


FROM {{ ref('users') }}
{% endsnapshot %}
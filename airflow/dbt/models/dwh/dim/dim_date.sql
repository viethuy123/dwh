{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_actual'], 'unique': True}
    ]
) }}

with date_series as (
    -- Sử dụng hàm đặc trưng của Postgres để gen chuỗi ngày
    select 
        generate_series(
            '2000-01-01'::date,
            (
                date_trunc('month', current_date)
                + interval '1 month - 1 day'
            )::date,
            '1 day'::interval
        )::date as date_actual
),

final as (
    select
        date_actual,
        extract(year from date_actual)::int as year_actual,
        extract(month from date_actual)::int as month_actual,
        extract(day from date_actual)::int as day_of_month,
        extract(quarter from date_actual)::int as quarter_actual,
        extract(isodow from date_actual)::int as day_of_week,
        
        -- Tên tháng và thứ
        trim(to_char(date_actual, 'Month')) as month_name,
        trim(to_char(date_actual, 'Day')) as day_name,
        
        -- Các cờ logic (Booleans)
        case when extract(isodow from date_actual) in (6, 7) then true else false end as is_weekend,
        
        -- Xác định ngày cuối tháng (Last day of month)
        (date_trunc('month', date_actual) + interval '1 month - 1 day')::date as last_day_of_month,
        
        -- Kiểm tra xem có phải ngày cuối tháng không
        case 
            when date_actual = (date_trunc('month', date_actual) + interval '1 month - 1 day')::date then true 
            else false 
        end as is_last_day_of_month
    from date_series
)

select * from final
order by date_actual
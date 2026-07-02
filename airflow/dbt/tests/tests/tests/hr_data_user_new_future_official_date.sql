select *
from {{ ref('hr_data_user_new') }}
where official_date > current_date
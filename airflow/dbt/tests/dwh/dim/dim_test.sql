select 1
from {{ ref('dim_test') }}
having count(*) = 0

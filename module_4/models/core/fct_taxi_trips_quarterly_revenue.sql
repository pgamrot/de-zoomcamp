with
    q_rev as (
        select
            service_type,
            trip_year as year,
            trip_quarter as quarter,
            sum(total_amount) as total_revenue
        from {{ ref("fact_trips") }}
        where trip_year IN (2019,2020)
        group by 1, 2, 3
        order by 1,2,3
    )
select
    q1.service_type,
    q1.year,
    q1.quarter,
    q1.total_revenue,
    q2.total_revenue as last_year_revenue,
    round(q1.total_revenue / nullif(q2.total_revenue, 0) - 1, 2) as yoy_growth_perc
from q_rev q1
left join q_rev q2 on q1.quarter = q2.quarter and q1.year = q2.year + 1 and q1.service_type=q2.service_type

{{ config(materialized="table") }}

with
    valid_trips as (
        select service_type, trip_year, trip_month, fare_amount
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')
    ),
    percentiles as (
        select
            service_type,
            trip_year,
            trip_month,
            percentile_cont(fare_amount, 0.97) over (
                partition by service_type, trip_year, trip_month
            ) as p97,
            percentile_cont(fare_amount, 0.95) over (
                partition by service_type, trip_year, trip_month
            ) as p95,
            percentile_cont(fare_amount, 0.90) over (
                partition by service_type, trip_year, trip_month
            ) as p90
        from valid_trips
    )
select
    service_type,
    trip_year,
    trip_month,
    max(p97) as p97,
    max(p95) as p95,
    max(p90) as p90
from percentiles
group by 1, 2, 3
order by 1, 2, 3

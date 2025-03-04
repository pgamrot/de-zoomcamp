{{ config(materialized="table") }}
select year, month, pickup_zone, dropoff_zone, MAX(p90_trip_duration) as p90_trip_duration from (
select
    year,
    month,
    pickup_zone,
    dropoff_zone,
    percentile_cont(trip_duration, 0.90) over (
        partition by year, month, pickup_zone, dropoff_zone
    ) as p90_trip_duration
from {{ ref("fact_fhv_trips") }}) sub
group by 1, 2, 3, 4

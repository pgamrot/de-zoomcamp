{{ config(materialized="table") }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv.*,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
    EXTRACT(YEAR FROM pickup_datetime) as year,
    EXTRACT(MONTH FROM pickup_datetime) as month
from {{ ref("stg_fhv_tripdata") }} as fhv
inner join dim_zones as pickup_zone on fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on fhv.dropoff_locationid = dropoff_zone.locationid

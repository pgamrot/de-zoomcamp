# Homework 4 solutions

## Question 1

Answer:
```select * from myproject.raw_nyc_tripdata.ext_green_taxi```

This is because DBT_BIGQUERY_PROJECT is taken from environment variables, but DBT_BIGQUERY_SOURCE_DATASET is not
defined (or at least we don't know) as env_var, and therefore taken from yaml file.

## Question 2

Answer: Update the WHERE clause to:
```pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY```

This results in following precedence hierarchy:

1. Command line arguments using --vars (accessible via var("days_back")) take the highest precedence
2. Environment variables (accessible via env_var("DAYS_BACK")) take second precedence
3. The default value of "30" is used if neither of the above is provided

## Question 3

Answer: ```dbt run --select models/staging/+```

This one is extremely confusing to me, but it is the only answer, in which dim_zone_lookup might not be materialized,
because it is not a direct child of the staging models.

## Question 4

Only the second answer is false, as the default env is target.

## Question 5

Answer D, clearly we can see the impact of COVID lockdown. The models are in the module_4 directory.

## Question 6

I actually got slightly different results, but close enough to answer B:
service_type	trip_year	trip_month	p97	p95	p90
Green	2020	4	55.0	45.0	26.5
Yellow	2020	4	32.0	26.0	19.0

## Question 7

Answer: LaGuardia Airport, Chinatown, Garment District

```
WITH ranking as (
SELECT pickup_zone, dropoff_zone, p90_trip_duration, ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) rn
FROM `eco-watch-233417.dbt_pgamrot.fct_fhv_monthly_zone_traveltime_p90` main WHERE pickup_zone IN ('Newark Airport','SoHo','Yorkville East') AND year=2019 and month=11 ORDER BY p90_trip_duration DESC)
SELECT pickup_zone, dropoff_zone, p90_trip_duration FROM ranking WHERE rn=2;
```

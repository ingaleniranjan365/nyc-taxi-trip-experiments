{{
    config(
        materialized='table'
    )
}}

with testing_trips as (
  select
      id,
      TIMESTAMP_DIFF(TIMESTAMP(pickup_datetime), TIMESTAMP '1970-01-01 00:00:00 UTC', SECOND) AS pickup_unix_time,
      passenger_count,
      pickup_longitude,
      pickup_latitude,
      dropoff_longitude,
      dropoff_latitude
  from {{ source('nyc_taxi_trip_duration','testing_trips') }}
)

select
  id,
  ROUND(predicted_trip_duration) AS trip_duration
from {{ dbt_ml.predict(ref('trip_duration_model'), 'testing_trips') }}
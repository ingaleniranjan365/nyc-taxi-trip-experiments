{{
    config(
        materialized='table'
    )
}}

with training_trips as (
  select * from {{ source('nyc_taxi_trip_duration','training_trips') }}
),

correlations as (
  select
    abs(corr(cast(trip_duration as numeric), TIMESTAMP_DIFF(timestamp(pickup_datetime), timestamp '1970-01-01 00:00:00 UTC', SECOND))) as pickup_datetime_corr,
    abs(corr(cast(trip_duration as numeric), passenger_count)) as passenger_count_corr,
    abs(corr(cast(trip_duration as numeric), pickup_longitude)) as pickup_longitude_corr,
    abs(corr(cast(trip_duration as numeric), pickup_latitude)) as pickup_latitude_corr,
    abs(corr(cast(trip_duration as numeric), dropoff_longitude)) as dropoff_longitude_corr,
    abs(corr(cast(trip_duration as numeric), dropoff_latitude)) as dropoff_latitude_corr
  from
    training_trips
)

select * from correlations

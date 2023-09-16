{{
    config(
        materialized='model',
        ml_config={
            'model_type': 'linear_reg',
            'input_label_cols': ['trip_duration']
        }
    )
}}

with training_trips as (
  select * from {{ source('nyc_taxi_trip_duration','training_trips') }}
)

SELECT
  trip_duration,
  TIMESTAMP_DIFF(TIMESTAMP(pickup_datetime), TIMESTAMP '1970-01-01 00:00:00 UTC', SECOND) AS pickup_unix_time,
  passenger_count,
  pickup_longitude,
  pickup_latitude,
  dropoff_longitude,
  dropoff_latitude
FROM
  training_trips
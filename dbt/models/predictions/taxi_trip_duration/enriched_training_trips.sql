{{ config(
    materialized='table'
) }}

with data as (
    select * from {{ source('nyc_taxi_trip_duration', 'training_trips') }}
),

enriched_data as (
    select
        *,
        {{ haversine_distance('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude') }},
        EXTRACT(HOUR FROM pickup_datetime) AS hour,
        EXTRACT(DAY FROM pickup_datetime) AS day,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        CASE
          WHEN store_and_fwd_flag = 'Y' THEN 1
          WHEN store_and_fwd_flag = 'N' THEN 0
        END AS store_and_fwd_flag_numeric
    from data
)

select
    id,
    vendor_id,
    passenger_count,
    store_and_fwd_flag_numeric as store_and_fwd_flag,
    trip_duration,
    distance,
    hour,
    day,
    month
from enriched_data

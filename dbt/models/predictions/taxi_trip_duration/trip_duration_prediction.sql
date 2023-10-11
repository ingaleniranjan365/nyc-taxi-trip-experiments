{{
    config(
        materialized='table'
    )
}}

with testing_trips as (
  select
    *
  from {{ ref('enriched_testing_trips') }}
)

select
  id,
  ROUND(predicted_trip_duration) AS trip_duration
from {{ dbt_ml.predict(ref('trip_duration_model'), 'testing_trips') }}
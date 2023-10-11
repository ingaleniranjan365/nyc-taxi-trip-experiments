{{
    config(
        materialized='model',
        ml_config={
            'model_type': 'boosted_tree_regressor',
            'input_label_cols': ['trip_duration']
        }
    )
}}

with training_trips as (
  select * from {{ ref('enriched_training_trips') }}
)

SELECT
    vendor_id,
    passenger_count,
    store_and_fwd_flag,
    distance,
    hour,
    day,
    month,
    trip_duration
FROM
  training_trips
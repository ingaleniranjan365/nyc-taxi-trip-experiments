with airport_trips as (
    select * from {{ ref('airport_trips') }}
),

airport_trips_by_dow AS (
  select
    airport,
    day_of_week,
    count(*) AS trip_count
  FROM
    airport_trips
  GROUP BY
    airport, day_of_week
  ORDER BY
    airport, day_of_week
)

select * from airport_trips_by_dow
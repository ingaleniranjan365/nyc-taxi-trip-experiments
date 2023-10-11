with airport_dropoffs as (
    select * from {{ ref('airport_dropoffs') }}
),

airport_dropoffs_by_dow AS (
  select
    airport,
    day_of_week,
    count(*) AS trip_count
  FROM
    airport_dropoffs
  GROUP BY
    airport, day_of_week
  ORDER BY
    airport, day_of_week
)

select * from airport_dropoffs_by_dow
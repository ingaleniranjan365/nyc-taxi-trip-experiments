with airport_dropoffs as (
    select * from {{ ref('airport_dropoffs') }}
),

airport_dropoffs_by_hod AS (
  select
    airport,
    hour_of_day,
    count(*) AS trip_count
  FROM
    airport_dropoffs
  GROUP BY
    airport, hour_of_day
  ORDER BY
    airport, hour_of_day
)

select * from airport_dropoffs_by_hod
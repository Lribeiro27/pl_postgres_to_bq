SELECT
  races.year,
  races.race_name,
  races.race_date,
  races.location,
  drivers.driver_name,
  drivers.driver_id,
  drivers.driver_nationality,
  constructors.constructor_name,
  results.grid,
  results.fastestLap,
  results.time,
  results.points
FROM
  `dbt-specialization.silver.stg_results` as results
  LEFT JOIN `dbt-specialization.silver.stg_constructors` as constructors
    ON results.constructor_id = constructors.constructor_id
  LEFT JOIN `dbt-specialization.silver.stg_drivers` as drivers
    ON results.driver_id = drivers.driver_id
  LEFT JOIN `dbt-specialization.silver.stg_races` as races
    ON results.race_id = races.race_id


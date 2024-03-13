SELECT
  resultId as result_id,
  raceId as race_id,
  driverId as driver_id,
  constructorId as constructor_id,
  number,
  grid,
  position,
  fastestLap,
  time,
  points
FROM
  `dbt-specialization.bronze.results`
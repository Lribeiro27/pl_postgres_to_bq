SELECT
  a.raceId as race_id,
  a.year,
  a.round,
  a.circuitId as circuit_id,
  a.name as race_name,
  a.date as race_date,
  b.circuitRef as circuit_ref,
  b.name as circuit_name,
  b.location,
  b.country
FROM
  {{source('bronze', 'races')}} as a
  LEFT JOIN {{source('bronze', 'circuits')}} as b
  ON a.circuitId = b.circuitId

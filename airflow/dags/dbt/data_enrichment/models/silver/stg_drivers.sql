SELECT
  driverId as driver_id,
  CONCAT(forename, " ", surname) as driver_name,
  dob as driver_date_born,
  nationality as driver_nationality
FROM
  {{source('bronze', 'drivers')}}
UNLOAD (
  SELECT *
  FROM (
    SELECT
      0 AS _order,
      'facility_id' AS facility_id,
      'facility_name' AS facility_name,
      'employee_count' AS employee_count,
      'number_of_offered_services' AS number_of_offered_services,
      'expiry_date_of_first_accreditation' AS expiry_date_of_first_accreditation
    UNION ALL
    SELECT
      1 AS _order,
      CAST(facility_id AS varchar),
      CAST(facility_name AS varchar),
      CAST(employee_count AS varchar),
      CAST(cardinality(services) AS varchar),
      CAST((
        SELECT MIN(CAST(a.valid_until AS DATE))
        FROM UNNEST(accreditations) AS t(a)
      ) AS varchar)
    FROM medlaunch_db.facilities_raw
  ) t
  ORDER BY _order, facility_id
)
TO 's3://medlaunch/export/facility_metrics/run_preview/'
WITH (
  format = 'TEXTFILE',
  field_delimiter = ',',
  compression = 'NONE'
);
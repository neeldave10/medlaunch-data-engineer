WITH parsed AS (
  SELECT
    facility_id,
    facility_name,
    employee_count,
    cardinality(services) AS number_of_offered_services,
    (
      SELECT MIN(CAST(a.valid_until AS DATE))
      FROM UNNEST(accreditations) AS t(a)
    ) AS expiry_date_of_first_accreditation
  FROM facilities_raw
)
SELECT *
FROM parsed;
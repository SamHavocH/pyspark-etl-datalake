-- Example analytics queries after running `datalake run`.

SELECT
  date_utc,
  avg_temperature_c,
  total_precipitation_mm,
  weather_profile
FROM gold_weather_daily_summary
ORDER BY date_utc DESC
LIMIT 14;

SELECT
  location_id,
  count(*) AS hourly_rows,
  min(observed_at_utc) AS first_observation,
  max(observed_at_utc) AS last_observation
FROM silver_weather_hourly
GROUP BY location_id;

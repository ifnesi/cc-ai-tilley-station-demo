-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 02_anomalies.sql — ML_DETECT_ANOMALIES on foot_in and alight_total
--
-- Runs over the windowed station_metrics stream. ML_DETECT_ANOMALIES is
-- forward-only (no retract), so the OVER window must be UNBOUNDED PRECEDING (a
-- bounded RANGE ... PRECEDING window would evict rows and demand retract).
-- State stays bounded via the function's own maxTrainingSize, not the SQL
-- window. Only is_anomaly rows are kept. Output ROW fields (per docs):
-- actual_value, forecast_value, lower_bound, upper_bound, is_anomaly.
-- occupancy / occupancy_pct are carried through so the downstream AI advisor
-- (06) has crowd context for anomaly-triggered suggestions.
INSERT INTO station_anomalies
  (window_start, metric, actual, forecast, lower_bound, upper_bound, is_anomaly, occupancy, occupancy_pct)
-- PARTITION BY station_name gives one model per station (a single partition
-- here, one modelled station) and clears MISSING_PARTITION_BY_FOR_OVER_WINDOW.
WITH metrics_keyed AS (
  -- station_name is a real column on station_metrics, so PARTITION BY it below is
  -- a genuine key (not a constant literal), clearing MISSING_PARTITION_BY.
  SELECT station_name, window_start, foot_in, alight_total, occupancy, occupancy_pct, `$rowtime`
  FROM station_metrics
),
foot_anom AS (
  SELECT
    window_start, occupancy, occupancy_pct,
    ML_DETECT_ANOMALIES(
      CAST(foot_in AS DOUBLE),
      window_start,
      JSON_OBJECT('minTrainingSize' VALUE ${min_training_size}, 'maxTrainingSize' VALUE 512, 'enableStl' VALUE false)
    ) OVER (
      PARTITION BY station_name
      ORDER BY `$rowtime`
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS r
  FROM metrics_keyed
),
alight_anom AS (
  SELECT
    window_start, occupancy, occupancy_pct,
    ML_DETECT_ANOMALIES(
      CAST(alight_total AS DOUBLE),
      window_start,
      JSON_OBJECT('minTrainingSize' VALUE ${min_training_size}, 'maxTrainingSize' VALUE 512, 'enableStl' VALUE false)
    ) OVER (
      PARTITION BY station_name
      ORDER BY `$rowtime`
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS r
  FROM metrics_keyed
)
SELECT window_start, 'foot_in' AS metric,
       r.actual_value, r.forecast_value, r.lower_bound, r.upper_bound, r.is_anomaly,
       occupancy, occupancy_pct
FROM foot_anom
WHERE r.is_anomaly
UNION ALL
SELECT window_start, 'alight_total' AS metric,
       r.actual_value, r.forecast_value, r.lower_bound, r.upper_bound, r.is_anomaly,
       occupancy, occupancy_pct
FROM alight_anom
WHERE r.is_anomaly;

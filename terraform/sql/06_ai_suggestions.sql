-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 06_ai_suggestions.sql — GenAI advisor -> station_ai_suggestions
--
-- Two trigger sources, UNION ALL, each debounced to one Bedrock call per
-- episode:
--   A) threshold episodes — occupancy_pct crossing ${pct_high} or
--      ${pct_critical} upward (LAG on pct, so one call per crossing);
--   B) anomaly episodes — a false->true is_anomaly transition on foot_in over
--      station_metrics (LAG), which also carries occupancy context.
-- The user prompt is assembled with CASE; current controls are derived
-- deterministically from pct (matching loops 03/04), avoiding temporal joins.
-- ML_PREDICT is called via LATERAL TABLE; the model OUTPUT column is `suggestion`
-- (aliased on the lateral). ML_PREDICT + task=text_generation matches the
-- Bedrock /invoke endpoint (mirrors the working ifnesi/retail-ai-demo); the
-- AI_COMPLETE path targets a different Bedrock API and 404s on /invoke.
INSERT INTO station_ai_suggestions
  (`trigger`, metric, severity, suggestion, event_time)
WITH
occ AS (
  SELECT
    station_name,
    occupancy,
    capacity,
    CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) AS pct,
    `$rowtime` AS event_time
  FROM station_occupancy
),
occ_lag AS (
  SELECT occ.*, LAG(pct) OVER (PARTITION BY station_name ORDER BY event_time) AS prev_pct FROM occ
),
threshold_trig AS (
  SELECT
    'threshold' AS `trigger`,
    'occupancy_pct' AS metric,
    'N/A' AS direction,
    CAST(occupancy AS DOUBLE) AS actual,
    ${pct_high} * CAST(capacity AS DOUBLE) AS forecast,
    CAST(occupancy AS DOUBLE) AS occupancy,
    CAST(capacity AS DOUBLE) AS capacity,
    pct, event_time
  FROM occ_lag
  WHERE (prev_pct < ${pct_high} AND pct >= ${pct_high})
     OR (prev_pct < ${pct_critical} AND pct >= ${pct_critical})
),
metrics_keyed AS (
  -- station_name is a real column on station_metrics, so PARTITION BY it in the
  -- OVER windows below is a genuine key (not a constant), clearing
  -- MISSING_PARTITION_BY_FOR_OVER_WINDOW.
  SELECT station_name, occupancy, occupancy_pct, foot_in, window_start, `$rowtime`
  FROM station_metrics
),
metrics_anom AS (
  SELECT
    station_name, occupancy, occupancy_pct AS pct, `$rowtime` AS event_time,
    ML_DETECT_ANOMALIES(
      CAST(foot_in AS DOUBLE), window_start,
      JSON_OBJECT('minTrainingSize' VALUE ${min_training_size}, 'maxTrainingSize' VALUE 512, 'enableStl' VALUE false)
    ) OVER (
      PARTITION BY station_name
      ORDER BY `$rowtime`
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS r
  FROM metrics_keyed
),
metrics_anom_lag AS (
  SELECT
    metrics_anom.*,
    LAG(CASE WHEN r.is_anomaly THEN 1 ELSE 0 END) OVER (PARTITION BY station_name ORDER BY event_time) AS prev_anom
  FROM metrics_anom
),
anomaly_trig AS (
  SELECT
    'anomaly' AS `trigger`,
    'foot_in' AS metric,
    'N/A' AS direction,
    r.actual_value AS actual,
    r.forecast_value AS forecast,
    CAST(occupancy AS DOUBLE) AS occupancy,
    CAST(${station_capacity} AS DOUBLE) AS capacity,
    pct, event_time
  FROM metrics_anom_lag
  WHERE r.is_anomaly AND (prev_anom IS NULL OR prev_anom = 0)
),
unified AS (
  SELECT * FROM threshold_trig
  UNION ALL
  SELECT * FROM anomaly_trig
),
prompted AS (
  SELECT
    `trigger`, metric, pct, event_time,
    CONCAT(
      'Alert. Station: Tilley. Trigger: ', `trigger`,
      '. Metric: ',
      CASE metric
        WHEN 'foot_in' THEN 'foot entries from the street'
        WHEN 'alight_total' THEN 'passengers alighting from trains'
        WHEN 'occupancy_pct' THEN 'station occupancy'
        ELSE metric
      END,
      '. Direction: ', direction, '. ',
      'Observed: ', CAST(CAST(ROUND(actual) AS INT) AS STRING),
      '. Expected around ', CAST(CAST(ROUND(forecast) AS INT) AS STRING),
      '. Occupancy: ', CAST(CAST(ROUND(occupancy) AS INT) AS STRING),
      ' of ', CAST(CAST(ROUND(capacity) AS INT) AS STRING),
      ' (', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%). ',
      'Status: ',
      CASE
        WHEN pct >= ${pct_critical} THEN 'CRITICAL'
        WHEN pct >= ${pct_high} THEN 'HIGH'
        ELSE 'ELEVATED'
      END,
      '. ',
      -- The station runs deterministic controls automatically (block signalling
      -- holds trains while a platform is occupied; the gateline throttles street
      -- inflow by occupancy). Give the duty supervisor the human judgement on
      -- top. The gateline band is a function of occupancy, so it is stated here.
      'Street gateline currently ',
      CASE
        WHEN pct >= ${pct_critical} THEN 'CLOSED'
        WHEN pct >= ${pct_high} THEN 'RESTRICTED'
        ELSE 'OPEN'
      END,
      '. Recommend the operational response for staff.'
    ) AS prompt
  FROM unified
)
SELECT
  p.`trigger`,
  p.metric,
  UPPER(COALESCE(NULLIF(REGEXP_EXTRACT(m.suggestion, 'SEVERITY: *([A-Za-z]+)', 1), ''), 'MEDIUM')) AS severity,
  m.suggestion,
  p.event_time
FROM prompted AS p
CROSS JOIN LATERAL TABLE(ML_PREDICT('${model_name}', p.prompt)) AS m(suggestion);

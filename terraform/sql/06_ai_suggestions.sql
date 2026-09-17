-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 06_ai_suggestions.sql — GenAI advisor -> station_ai_suggestions
--
-- The AI is EXPENSIVE, so ML_PREDICT (the Bedrock call) fires only on meaningful
-- events, from two sources UNION ALL'd (no join):
--   A) ANOMALY episodes — consumed straight from station_anomalies, the output of
--      02 (the single place ML_DETECT_ANOMALIES runs). Each anomalous window is
--      worth advising on, and 02 is windowed so this is at most ~1 per 15s.
--   B) OCCUPANCY band step-ups — from the windowed station_metrics, debounced on
--      the band (LAG), so a plateau just above 85% does not re-fire every window.
--
-- Both anomaly and threshold advice reach the ops team; the anomaly ones add to
-- the occupancy-driven ones. ML_PREDICT + task=text_generation matches the
-- Bedrock /invoke endpoint (mirrors ifnesi/retail-ai-demo); AI_COMPLETE 404s.
INSERT INTO station_ai_suggestions
  (`trigger`, metric, severity, suggestion, event_time)
WITH
-- A) anomalies: consume the ML output directly (single source of detection).
anomaly_trig AS (
  SELECT
    'anomaly' AS `trigger`,
    metric,
    actual,
    forecast,
    occupancy,
    occupancy_pct AS pct,
    `$rowtime` AS event_time
  FROM station_anomalies
),
-- B) occupancy band step-ups, debounced on the band change.
occ AS (
  SELECT
    station_name,
    occupancy,
    occupancy_pct AS pct,
    `$rowtime` AS event_time,
    CASE
      WHEN occupancy_pct >= ${pct_critical} THEN 3
      WHEN occupancy_pct >= ${pct_high} THEN 2
      WHEN occupancy_pct >= ${pct_low} THEN 1
      ELSE 0
    END AS band
  FROM station_metrics
),
occ_lag AS (
  SELECT occ.*, LAG(band) OVER (PARTITION BY station_name ORDER BY event_time) AS prev_band
  FROM occ
),
threshold_trig AS (
  SELECT
    'threshold' AS `trigger`,
    'occupancy_pct' AS metric,
    CAST(occupancy AS DOUBLE) AS actual,
    ${pct_high} * CAST(${station_capacity} AS DOUBLE) AS forecast,
    occupancy,
    pct,
    event_time
  FROM occ_lag
  -- Fire on stepping UP into BUSY (early warning), HIGH, or CRITICAL — one call
  -- per step up, never while the band is unchanged.
  WHERE band >= 1 AND (prev_band IS NULL OR band > prev_band)
),
unified AS (
  SELECT `trigger`, metric, actual, forecast, occupancy, pct, event_time FROM anomaly_trig
  UNION ALL
  SELECT `trigger`, metric, actual, forecast, occupancy, pct, event_time FROM threshold_trig
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
      '. Observed: ', CAST(CAST(ROUND(actual) AS INT) AS STRING),
      '. Expected around ', CAST(CAST(ROUND(forecast) AS INT) AS STRING),
      '. Occupancy: ', CAST(CAST(ROUND(CAST(occupancy AS DOUBLE)) AS INT) AS STRING),
      ' of ', CAST(${station_capacity} AS STRING),
      ' (', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%). ',
      'Status: ',
      CASE
        WHEN pct >= ${pct_critical} THEN 'CRITICAL'
        WHEN pct >= ${pct_high} THEN 'HIGH'
        WHEN pct >= ${pct_low} THEN 'BUSY'
        ELSE 'NOMINAL'
      END,
      '. ',
      -- The station runs deterministic controls automatically (block signalling
      -- holds trains while a platform is occupied; the gateline throttles street
      -- inflow by occupancy). The gateline band is a function of occupancy, so it
      -- is stated here; the AI adds the human judgement for staff on top.
      'Street gateline currently ',
      CASE WHEN pct >= ${pct_critical} THEN 'CLOSED' WHEN pct >= ${pct_high} THEN 'RESTRICTED' ELSE 'OPEN' END,
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

-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 06_ai_suggestions.sql — GenAI advisor -> station_ai_suggestions
--
-- The AI is EXPENSIVE, so ML_PREDICT (the Bedrock call) fires only on meaningful
-- events, from two sources UNION ALL'd (no join):
--   A) ANOMALY episodes — consumed from station_anomalies, the output of 02 (the
--      single place ML_DETECT_ANOMALIES runs). Repeated anomalies for one metric
--      form one episode; a new call is allowed after a configured quiet gap.
--   B) OCCUPANCY bands — directly from the authoritative station_occupancy
--      heartbeat, avoiding an intermediate transactional topic on the
--      latency-sensitive safety path. One intentional
--      cadence: a step UP into BUSY, HIGH, or CRITICAL fires once. LAG debounces
--      plateaus, preventing a five-second window from becoming an unbounded stream
--      of paid Bedrock calls during a sustained incident.
--
-- Both anomaly and threshold advice reach the ops team; the anomaly ones add to
-- the occupancy-driven ones. ML_PREDICT + task=text_generation matches the
-- Bedrock /invoke endpoint (mirrors ifnesi/retail-ai-demo); AI_COMPLETE 404s.
INSERT INTO station_ai_suggestions
  (`trigger`, metric, severity, suggestion, event_time)
WITH
-- A) anomalies: consume the ML output directly (single source of detection),
-- but retain the previous anomaly time per metric to identify episode boundaries.
anomaly_base AS (
  SELECT
    'anomaly' AS `trigger`,
    metric,
    actual,
    forecast,
    occupancy,
    occupancy_pct AS pct,
    window_start,
    `$rowtime` AS event_time
  FROM station_anomalies
),
anomaly_lag AS (
  SELECT
    anomaly_base.*,
    LAG(window_start) OVER (PARTITION BY metric ORDER BY event_time) AS prev_anomaly_window
  FROM anomaly_base
),
anomaly_trig AS (
  SELECT `trigger`, metric, actual, forecast, occupancy, pct, event_time
  FROM anomaly_lag
  WHERE prev_anomaly_window IS NULL
     OR window_start >= prev_anomaly_window + INTERVAL '${anomaly_cooldown}' SECOND
),
-- B) occupancy band step-ups, debounced on the band change.
occ AS (
  SELECT
    station_name,
    occupancy,
    COALESCE(
      CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0),
      0.0
    ) AS pct,
    `$rowtime` AS event_time,
    CASE
      WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_critical} THEN 3
      WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_high} THEN 2
      WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_low} THEN 1
      ELSE 0
    END AS band
  FROM station_occupancy
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
    -- "Expected around" for a threshold trigger is the headcount at the band the
    -- event actually crossed, not always pct_high — so the prompt context matches
    -- the severity (BUSY/HIGH/CRITICAL) instead of implying an 85% baseline.
    CAST(
      CASE band
        WHEN 3 THEN ${pct_critical}
        WHEN 2 THEN ${pct_high}
        ELSE ${pct_low}
      END * ${station_capacity} AS DOUBLE
    ) AS forecast,
    occupancy,
    pct,
    event_time
  FROM occ_lag
  -- Fire once when stepping up into BUSY, HIGH, or CRITICAL. A plateau does not
  -- call the remote model again; the dashboard retains the latest advice.
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
  -- Constrain the parsed severity to the known enum. REGEXP_EXTRACT can return any
  -- alphabetic token after 'SEVERITY:', so map anything unexpected to MEDIUM rather
  -- than let a stray word (or empty match) flow downstream to the dashboard.
  CASE UPPER(COALESCE(NULLIF(REGEXP_EXTRACT(m.suggestion, 'SEVERITY: *([A-Za-z]+)', 1), ''), 'MEDIUM'))
    WHEN 'LOW' THEN 'LOW'
    WHEN 'HIGH' THEN 'HIGH'
    ELSE 'MEDIUM'
  END AS severity,
  m.suggestion,
  p.event_time
FROM prompted AS p
CROSS JOIN LATERAL TABLE(
  ML_PREDICT(
    '${model_name}',
    p.prompt,
    map[
      'async_enabled', true,
      'client_timeout', ${ai_client_timeout},
      'max_parallelism', ${ai_max_parallelism},
      'retry_count', ${ai_retry_count}
    ]
  )
) AS m(suggestion);

-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 04_gateline_control.sql — control loop 2 -> gateline_state
--
-- The real cap on total occupancy: throttle the unbounded street inflow.
-- Keys off the latest station_occupancy heartbeat. evac_time = occupancy /
-- ${evacuation_flow}.
--   pct >= ${pct_critical} OR evac >= ${evac_crit} -> CLOSED     (0.0)
--   pct >= ${pct_high}     OR evac >= ${evac_warn} -> RESTRICTED (0.4)
--   otherwise                                       -> OPEN       (1.0)
-- Emit only on state change (LAG, bounded state); re-opens as values fall back.
INSERT INTO gateline_state
  (station_name, state, throttle_factor, reason, event_time)
SELECT
  station_name,
  state,
  CASE state
    WHEN 'CLOSED' THEN CAST(0.0 AS FLOAT)
    WHEN 'RESTRICTED' THEN CAST(0.4 AS FLOAT)
    ELSE CAST(1.0 AS FLOAT)
  END AS throttle_factor,
  CASE state
    WHEN 'CLOSED' THEN CONCAT('CRITICAL ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%: closing the street gateline')
    WHEN 'RESTRICTED' THEN CONCAT('Occupancy ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '% over HIGH: restricting street inflow')
    ELSE CONCAT('Occupancy ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '% nominal: gateline open')
  END AS reason,
  event_time
FROM (
  SELECT
    station_name, pct, state, event_time,
    LAG(state) OVER (PARTITION BY station_name ORDER BY event_time) AS prev_state
  FROM (
    SELECT
      station_name,
      CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) AS pct,
      CASE
        WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_critical}
             OR CAST(occupancy AS DOUBLE) / ${evacuation_flow} >= ${evac_crit} THEN 'CLOSED'
        WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_high}
             OR CAST(occupancy AS DOUBLE) / ${evacuation_flow} >= ${evac_warn} THEN 'RESTRICTED'
        ELSE 'OPEN'
      END AS state,
      `$rowtime` AS event_time
    FROM station_occupancy
  )
)
WHERE prev_state IS NULL OR state <> prev_state;

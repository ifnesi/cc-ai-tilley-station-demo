-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 03_signal_control.sql — control loop 1 -> signal_state
--
-- Keys off the latest station_occupancy heartbeat (~2-5s), not the 15s window,
-- so signals react within a few seconds. Occupancy bands (occupancy-driven so
-- the "signals hold" beat fires deterministically during a surge):
--   pct >= ${pct_critical} -> GREEN  (deadlock-breaker: trains are the drain)
--   pct >= ${pct_high}     -> RED    (meter arrivals while the concourse is jammed)
--   otherwise              -> GREEN
-- Emit only on state change (LAG over the occupancy stream, per station key,
-- bounded state). Both sides share the occupancy-driven decision, fanned out
-- with UNION ALL (not a keyless CROSS JOIN) -> one row per side per change.
INSERT INTO signal_state
  (station_name, side, state, reason, occupancy_at_decision, event_time)
WITH decided AS (
  SELECT
    station_name,
    occupancy,
    CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) AS pct,
    CASE
      WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_critical} THEN 'GREEN'
      WHEN CAST(occupancy AS DOUBLE) / NULLIF(CAST(capacity AS DOUBLE), 0) >= ${pct_high} THEN 'RED'
      ELSE 'GREEN'
    END AS state,
    `$rowtime` AS event_time
  FROM station_occupancy
),
changed AS (
  SELECT
    station_name, occupancy, pct, state, event_time,
    LAG(state) OVER (PARTITION BY station_name ORDER BY event_time) AS prev_state
  FROM decided
),
transitions AS (
  SELECT
    station_name,
    state,
    occupancy AS occupancy_at_decision,
    event_time,
    CASE
      WHEN state = 'GREEN' AND pct >= ${pct_critical}
        THEN CONCAT('CRITICAL ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%: releasing trains to board people out')
      WHEN state = 'RED'
        THEN CONCAT('Holding arrivals: occupancy ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%, concourse control')
      ELSE
        CONCAT('Occupancy ', CAST(CAST(ROUND(pct * 100) AS INT) AS STRING), '%: normal running')
    END AS reason
  FROM changed
  WHERE prev_state IS NULL OR state <> prev_state
)
SELECT station_name, 'LEFT' AS side, state, reason, occupancy_at_decision, event_time FROM transitions
UNION ALL
SELECT station_name, 'RIGHT' AS side, state, reason, occupancy_at_decision, event_time FROM transitions;

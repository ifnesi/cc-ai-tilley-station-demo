-- (watermark idleness handled by sql.tables.scan.idle-timeout statement property)
-- 01_metrics.sql — windowed crowd metrics -> station_metrics
--
-- Single APPEND-ONLY windowed aggregation: UNION ALL the four sources into one
-- normalised stream (each event contributes its columns, 0/NULL elsewhere),
-- then ONE ${window_seconds}s TUMBLE aggregation. This avoids joining
-- pre-aggregated windows, which Flink treats as a retracting regular join that
-- an append-only Kafka sink cannot consume.
--
-- UNION ALL preserves the `$rowtime` time attribute (aliased `ts`) across all
-- inputs, so the outer TUMBLE has a valid time attribute. Occupancy is a level,
-- not a sum: take the latest occupancy heartbeat in the window with LAST_VALUE
-- (COALESCE to MAX as a null-safe fallback).
--
-- Templated by Terraform (templatefile) — placeholders are substituted.
INSERT INTO station_metrics
  (station_name, window_start, window_end, foot_in, alight_wait, alight_exit,
   alight_total, board_total, alight_east, alight_west, board_east, board_west,
   net_change, occupancy, occupancy_pct, evac_time)
-- station_name is the per-station GROUP BY key (metrics are per station). This
-- models one station today, so it is a single partition, but keeping the key
-- makes the aggregation partitioned/parallelisable and future-proofs multiple
-- stations (clears WINDOW_AGGREGATION_WITHOUT_KEY).
WITH unified AS (
  -- street foot entries -> foot_in
  SELECT
    station_name,
    `$rowtime` AS ts,
    CAST(passengers AS INT) AS foot_in,
    0 AS alight_wait, 0 AS alight_exit,
    0 AS board_total,
    0 AS alight_east, 0 AS alight_west, 0 AS board_east, 0 AS board_west,
    CAST(NULL AS INT) AS occ, CAST(NULL AS INT) AS cap
  FROM passengers_flow
  UNION ALL
  -- alighting passengers -> alight_wait / alight_exit (+ per direction)
  SELECT
    '${station_name}',
    `$rowtime`,
    0,
    CAST(passengers_station AS INT),
    CAST(passengers_out AS INT),
    0,
    CAST(CASE WHEN direction_of_travel = 'east_bound' THEN passengers_out + passengers_station ELSE 0 END AS INT),
    CAST(CASE WHEN direction_of_travel = 'west_bound' THEN passengers_out + passengers_station ELSE 0 END AS INT),
    0, 0,
    CAST(NULL AS INT), CAST(NULL AS INT)
  FROM train_in_station
  UNION ALL
  -- departing trains -> board_total (+ per direction). Departures only.
  SELECT
    '${station_name}',
    `$rowtime`,
    0, 0, 0,
    CAST(passengers_boarding AS INT),
    0, 0,
    CAST(CASE WHEN direction_of_travel = 'east_bound' THEN passengers_boarding ELSE 0 END AS INT),
    CAST(CASE WHEN direction_of_travel = 'west_bound' THEN passengers_boarding ELSE 0 END AS INT),
    CAST(NULL AS INT), CAST(NULL AS INT)
  FROM train_in_transit
  WHERE next_stop <> '${station_name}' AND passengers_boarding IS NOT NULL
  UNION ALL
  -- authoritative occupancy heartbeat -> occ / cap
  SELECT
    station_name,
    `$rowtime`,
    0, 0, 0, 0, 0, 0, 0, 0,
    CAST(occupancy AS INT), CAST(capacity AS INT)
  FROM station_occupancy
)
SELECT
  station_name,
  window_start,
  window_end,
  CAST(SUM(foot_in) AS INT) AS foot_in,
  CAST(SUM(alight_wait) AS INT) AS alight_wait,
  CAST(SUM(alight_exit) AS INT) AS alight_exit,
  CAST(SUM(alight_wait) + SUM(alight_exit) AS INT) AS alight_total,
  CAST(SUM(board_total) AS INT) AS board_total,
  CAST(SUM(alight_east) AS INT) AS alight_east,
  CAST(SUM(alight_west) AS INT) AS alight_west,
  CAST(SUM(board_east) AS INT) AS board_east,
  CAST(SUM(board_west) AS INT) AS board_west,
  CAST(SUM(foot_in) + SUM(alight_wait) - SUM(board_total) AS INT) AS net_change,
  CAST(COALESCE(LAST_VALUE(occ), MAX(occ), 0) AS INT) AS occupancy,
  CAST(COALESCE(LAST_VALUE(occ), MAX(occ), 0) AS DOUBLE) / NULLIF(CAST(MAX(cap) AS DOUBLE), 0) AS occupancy_pct,
  CAST(COALESCE(LAST_VALUE(occ), MAX(occ), 0) AS DOUBLE) / ${evacuation_flow} AS evac_time
FROM TABLE(TUMBLE(TABLE unified, DESCRIPTOR(ts), INTERVAL '${window_seconds}' SECOND))
GROUP BY window_start, window_end, station_name;

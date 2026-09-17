-- 07_operator_qa.sql — operator Ask-AI: question + live context -> Bedrock.
--
-- The dashboard publishes a question to operator_questions. Flink enriches it
-- with the LATEST station_metrics and station_anomalies for the station via a
-- TEMPORAL JOIN (FOR SYSTEM_TIME AS OF the question time), so the AI answers with
-- current crowd context, then writes the answer to operator_answers keyed by
-- question_id. Deduplication (ROW_NUMBER = 1, ORDER BY time DESC) turns the
-- append-only metrics/anomaly streams into versioned tables the temporal join can
-- look up. LEFT joins so a question is always answered even before context exists.
INSERT INTO operator_answers
  (question_id, question, answer, event_time)
WITH
metrics_latest AS (
  SELECT station_name, occupancy, occupancy_pct, foot_in, alight_total, board_total, `$rowtime`
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY station_name ORDER BY `$rowtime` DESC) AS rn
    FROM station_metrics
  ) WHERE rn = 1
),
anomaly_latest AS (
  SELECT station_name, metric, actual, forecast, `$rowtime`
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY station_name ORDER BY `$rowtime` DESC) AS rn
    FROM station_anomalies
  ) WHERE rn = 1
),
enriched AS (
  SELECT
    q.question_id,
    q.question,
    q.`$rowtime` AS event_time,
    m.occupancy,
    m.occupancy_pct,
    m.foot_in,
    m.alight_total,
    m.board_total,
    a.metric AS anom_metric,
    a.actual AS anom_actual,
    a.forecast AS anom_forecast
  FROM operator_questions AS q
  LEFT JOIN metrics_latest FOR SYSTEM_TIME AS OF q.`$rowtime` AS m
    ON q.station_name = m.station_name
  LEFT JOIN anomaly_latest FOR SYSTEM_TIME AS OF q.`$rowtime` AS a
    ON q.station_name = a.station_name
),
prompted AS (
  SELECT
    question_id, question, event_time,
    CONCAT(
      'Question from the duty supervisor: ', question,
      ' | Live context for Tilley Station -- Occupancy: ',
      COALESCE(CAST(CAST(ROUND(CAST(occupancy AS DOUBLE)) AS INT) AS STRING), 'n/a'),
      ' of ', CAST(${station_capacity} AS STRING),
      ' (', COALESCE(CAST(CAST(ROUND(occupancy_pct * 100) AS INT) AS STRING), '?'), '%)',
      '; foot entries last window: ', COALESCE(CAST(foot_in AS STRING), '0'),
      '; alighting last window: ', COALESCE(CAST(alight_total AS STRING), '0'),
      '; boarding last window: ', COALESCE(CAST(board_total AS STRING), '0'),
      '; latest anomaly: ',
      COALESCE(CONCAT(anom_metric, ' observed ', CAST(CAST(ROUND(anom_actual) AS INT) AS STRING),
                      ' vs expected ', CAST(CAST(ROUND(anom_forecast) AS INT) AS STRING)), 'none'),
      '. Answer the question using this live context.'
    ) AS prompt
  FROM enriched
)
SELECT
  p.question_id,
  p.question,
  m.answer,
  p.event_time
FROM prompted AS p
CROSS JOIN LATERAL TABLE(ML_PREDICT('${qa_model_name}', p.prompt)) AS m(answer);

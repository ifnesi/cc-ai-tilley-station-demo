-- 07_operator_qa.sql — operator Ask-AI: question + live context -> Bedrock.
--
-- The dashboard publishes a question to operator_questions. The backend has
-- already snapshotted the live crowd state (it consumes station_metrics and
-- station_anomalies for the dashboard) into the record's `context` field, so this
-- job stays simple and append-only: build the prompt from the question + context
-- and call the conversational Bedrock model (05b) via ML_PREDICT, writing the
-- answer to operator_answers keyed by question_id.
INSERT INTO operator_answers
  (question_id, question, answer, event_time)
SELECT
  q.question_id,
  q.question,
  m.answer,
  q.event_time
FROM operator_questions AS q
CROSS JOIN LATERAL TABLE(
  ML_PREDICT(
    '${qa_model_name}',
    CONCAT(
      'You are advising the duty supervisor at Tilley Station. Question: ',
      q.question,
      '. Live station context: ',
      CASE WHEN q.context IS NULL OR q.context = '' THEN 'not available' ELSE q.context END,
      '. Answer the question using this context.'
    )
  )
) AS m(answer);

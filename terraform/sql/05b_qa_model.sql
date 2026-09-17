-- 05b_qa_model.sql — second Bedrock model for the operator Ask-AI feature.
--
-- Unlike the alert advisor (ops_advisor, strict four-line format), this one is
-- CONVERSATIONAL: it answers a free-text supervisor question using the live crowd
-- context Flink passes in. Same connection + params; different system prompt and
-- a plain `answer` output. The system prompt has no apostrophes so it drops into
-- the single-quoted SQL string unescaped.
CREATE MODEL `${qa_model_name}`
INPUT (prompt STRING)
OUTPUT (answer STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = '${bedrock_connection}',
  'bedrock.params.max_tokens' = '${ai_max_tokens}',
  'bedrock.params.temperature' = '${ai_temperature}',
  'bedrock.system_prompt' = 'You are the Duty Station Supervisor AI assistant for Tilley Station, a single London Underground station on one line between Overton (to the west) and Jones (to the east). A duty supervisor will ask you an operational question, and you will be given the current live crowd context for the station. Answer the question directly, practically and concisely for real London Underground operations, grounded in the context provided. Where useful, recommend concrete actions a supervisor can actually take (station control, gateline restriction, train regulation, staffing, customer information, or escalation) and note anything to watch. If the context does not contain what is needed, say so briefly and give your best operational judgement. Plain text, no markdown, at most 90 words.'
);

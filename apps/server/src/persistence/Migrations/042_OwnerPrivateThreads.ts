import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS owner_private_threads (thread_id TEXT PRIMARY KEY)`;
  yield* sql`
    INSERT OR IGNORE INTO owner_private_threads (thread_id)
    SELECT thread_id FROM projection_thread_sessions WHERE provider_name = 'pi'
  `;
  yield* sql`
    INSERT OR IGNORE INTO owner_private_threads (thread_id)
    SELECT stream_id FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND json_valid(payload_json)
      AND json_extract(payload_json, '$.session.providerName') = 'pi'
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS owner_private_session_insert
    AFTER INSERT ON projection_thread_sessions WHEN NEW.provider_name = 'pi'
    BEGIN INSERT OR IGNORE INTO owner_private_threads (thread_id) VALUES (NEW.thread_id); END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS owner_private_session_update
    AFTER UPDATE ON projection_thread_sessions WHEN NEW.provider_name = 'pi'
    BEGIN INSERT OR IGNORE INTO owner_private_threads (thread_id) VALUES (NEW.thread_id); END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS owner_private_event_insert
    AFTER INSERT ON orchestration_events
    WHEN NEW.aggregate_kind = 'thread' AND json_valid(NEW.payload_json)
      AND json_extract(NEW.payload_json, '$.session.providerName') = 'pi'
    BEGIN INSERT OR IGNORE INTO owner_private_threads (thread_id) VALUES (NEW.stream_id); END
  `;
});

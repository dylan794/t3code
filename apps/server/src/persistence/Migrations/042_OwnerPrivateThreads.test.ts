import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderInstanceId,
  ProjectId,
  ThreadId,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { makeDurableOwnerPresentation } from "../../provider/pi/OwnerThreadPresentation.ts";

const layer = it.layer(NodeSqliteClient.layerMemory());
layer("042_OwnerPrivateThreads", (it) => {
  it.effect(
    "keeps historical Pi content private after provider replacement and presentation restart",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 41 });
        // Only the archived event identifies the former Pi session.
        yield* sql`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
        VALUES ('historical-pi', 'thread', 'historical', 1, 'thread.session-set', '2026-10-06T00:00:00Z', 'system', '{"session":{"providerName":"pi"}}', '{}')`;
        yield* runMigrations({ toMigrationInclusive: 42 });
        const threadId = ThreadId.make("historical");
        const thread: OrchestrationThreadShell = {
          id: threadId,
          projectId: ProjectId.make("synthetic-project"),
          title: "PRIVATE-OWNER-TITLE",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "synthetic" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "PRIVATE-BRANCH",
          worktreePath: "PRIVATE-PATH",
          latestTurn: null,
          createdAt: "2026-10-06T00:00:00Z",
          updatedAt: "2026-10-06T00:00:00Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          session: null,
          latestUserMessageAt: null,
          hasPendingApprovals: true,
          hasPendingUserInput: true,
          hasActionableProposedPlan: true,
        };
        const restarted = yield* makeDurableOwnerPresentation;
        const presented = yield* Effect.promise(() => restarted.presentShellThread(thread));
        assert.equal(presented.title, "Locked Jarvis thread");
        assert.equal(presented.branch, null);
        assert.equal(presented.hasPendingApprovals, false);
        const command = {
          type: "thread.meta.update" as const,
          commandId: CommandId.make("switch-private"),
          threadId,
          modelSelection: thread.modelSelection,
        };
        const rejected = yield* restarted.validateDestination(command, "codex").pipe(Effect.flip);
        assert.equal(rejected._tag, "OrchestrationCommandInvariantError");
        yield* restarted.validateDestination(command, "pi");
        const unmarked = { ...command, threadId: ThreadId.make("ordinary") };
        yield* restarted.validateDestination(unmarked, "codex");
        yield* restarted.validateDestination(
          { ...unmarked, threadId: ThreadId.make("selected-pi") },
          "pi",
        );
        assert.equal(
          yield* Effect.promise(() => restarted.privateHistory(ThreadId.make("selected-pi"))),
          true,
        );

        yield* sql`INSERT INTO projection_thread_sessions (thread_id, status, provider_name, updated_at, runtime_mode)
        VALUES ('new-pi', 'ready', 'pi', '2026-10-06T00:00:00Z', 'full-access')`;
        yield* sql`UPDATE projection_thread_sessions SET provider_name = 'codex' WHERE thread_id = 'new-pi'`;
        yield* sql`DELETE FROM projection_thread_sessions WHERE thread_id = 'new-pi'`;
        const nextPresentation = yield* makeDurableOwnerPresentation;
        assert.equal(
          yield* Effect.promise(() => nextPresentation.privateHistory(ThreadId.make("new-pi"))),
          true,
        );
        yield* sql`INSERT INTO projection_thread_sessions (thread_id, status, provider_name, updated_at, runtime_mode)
        VALUES ('updated-pi', 'ready', 'codex', '2026-10-06T00:00:00Z', 'full-access')`;
        yield* sql`UPDATE projection_thread_sessions SET provider_name = 'pi' WHERE thread_id = 'updated-pi'`;
        assert.equal(
          yield* Effect.promise(() => nextPresentation.privateHistory(ThreadId.make("updated-pi"))),
          true,
        );
        yield* sql`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
        VALUES ('new-event-pi', 'thread', 'event-pi', 1, 'thread.session-set', '2026-10-06T00:00:00Z', 'system', '{"session":{"providerName":"pi"}}', '{}')`;
        assert.equal(
          yield* Effect.promise(() => nextPresentation.privateHistory(ThreadId.make("event-pi"))),
          true,
        );
        yield* sql`DROP TABLE owner_private_threads`;
        assert.equal(
          (yield* Effect.promise(() => nextPresentation.presentShellThread(thread))).title,
          "Locked Jarvis thread",
        );
      }),
  );
});

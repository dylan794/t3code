import type {
  OrchestrationShellSnapshot,
  OrchestrationThread,
  OrchestrationThreadShell,
  OrchestrationThreadDetailSnapshot,
  OrchestrationCommand,
  ThreadId,
} from "@t3tools/contracts";
import { mayPresentJarvisOwner } from "./JarvisOwnerCore.ts";
import { ownerPrivateChannel } from "./OwnerPrivateChannel.ts";
import { projectThreadDetailSnapshot } from "../../orchestration/ActivityPayloadProjection.ts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { OrchestrationCommandInvariantError } from "../../orchestration/Errors.ts";
type PrivateHistory = (threadId: ThreadId) => Promise<boolean>;
const piInstances = new Set<string>(["pi"]);
const otherInstances = new Set<string>(["codex", "claude", "cursor", "grok", "opencode"]);
export function rememberPiInstances(instances: Record<string, { driver: string }>): void {
  for (const [id, instance] of Object.entries(instances)) {
    if (instance.driver === "pi") piInstances.add(id);
    else otherInstances.add(id);
  }
}

export function privateThread(thread: {
  id: ThreadId;
  session?: { providerName: string | null } | null;
  modelSelection?: { instanceId: string };
}): boolean {
  return (
    thread.session?.providerName === "pi" ||
    (!!thread.modelSelection &&
      (piInstances.has(thread.modelSelection.instanceId) ||
        !otherInstances.has(thread.modelSelection.instanceId))) ||
    ownerPrivateChannel.concealed(thread.id)
  );
}
export async function permittedThread(
  thread: {
    id: ThreadId;
    session?: { providerName: string | null } | null;
    modelSelection?: { instanceId: string };
  },
  clientId?: string,
  privateHistory?: PrivateHistory,
): Promise<boolean> {
  return (
    !(privateThread(thread) || (await privateHistory?.(thread.id))) ||
    (!!clientId && (await mayPresentJarvisOwner(thread.id, clientId)))
  );
}
export async function presentThread(
  thread: OrchestrationThread,
  clientId?: string,
  privateHistory?: PrivateHistory,
): Promise<OrchestrationThread> {
  if (await permittedThread(thread, clientId, privateHistory)) return thread;
  return {
    ...thread,
    title: "Locked Jarvis thread",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    messages: [],
    activities: [],
    checkpoints: [],
    proposedPlans: [],
    session: null,
    titleRegeneration: null,
  };
}
export async function presentThreadSnapshot(
  snapshot: OrchestrationThreadDetailSnapshot,
  clientId?: string,
  privateHistory?: PrivateHistory,
): Promise<OrchestrationThreadDetailSnapshot> {
  const projected = projectThreadDetailSnapshot(snapshot);
  return { ...projected, thread: await presentThread(projected.thread, clientId, privateHistory) };
}
export async function presentShellThread(
  thread: OrchestrationThreadShell,
  clientId?: string,
  privateHistory?: PrivateHistory,
): Promise<OrchestrationThreadShell> {
  if (await permittedThread(thread, clientId, privateHistory)) return thread;
  return {
    ...thread,
    title: "Locked Jarvis thread",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    session: null,
    titleRegeneration: null,
    planProgress: null,
    backgroundLiveness: null,
  };
}
export async function presentShell(
  snapshot: OrchestrationShellSnapshot,
  clientId?: string,
  privateHistory?: PrivateHistory,
): Promise<OrchestrationShellSnapshot> {
  return {
    ...snapshot,
    threads: await Promise.all(
      snapshot.threads.map((thread) => presentShellThread(thread, clientId, privateHistory)),
    ),
  };
}
export const makeDurableOwnerPresentation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const runPromise = Effect.runPromiseWith(yield* Effect.context<SqlClient.SqlClient>());
  const privateHistory: PrivateHistory = async (threadId) => {
    try {
      const rows = await runPromise(
        sql`SELECT thread_id FROM owner_private_threads WHERE thread_id = ${threadId}`,
      );
      return rows.length > 0;
    } catch {
      return true;
    }
  };
  return {
    privateHistory,
    validateDestination: (command: OrchestrationCommand, driver: string | undefined) =>
      Effect.gen(function* () {
        if (
          command.type !== "thread.turn.start" &&
          command.type !== "thread.meta.update" &&
          command.type !== "thread.create"
        )
          return;
        if (command.type === "thread.meta.update" && !command.modelSelection) return;
        if (driver === "pi") {
          yield* sql`INSERT OR IGNORE INTO owner_private_threads (thread_id) VALUES (${command.threadId})`;
          return;
        }
        if (yield* Effect.promise(() => privateHistory(command.threadId)))
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail:
              "Private Jarvis history must remain in a Pi thread. Start a separate thread for another provider.",
          });
      }),
    permittedThread: (thread: Parameters<typeof permittedThread>[0], clientId?: string) =>
      permittedThread(thread, clientId, privateHistory),
    presentThread: (thread: OrchestrationThread, clientId?: string) =>
      presentThread(thread, clientId, privateHistory),
    presentThreadSnapshot: (snapshot: OrchestrationThreadDetailSnapshot, clientId?: string) =>
      presentThreadSnapshot(snapshot, clientId, privateHistory),
    presentShellThread: (thread: OrchestrationThreadShell, clientId?: string) =>
      presentShellThread(thread, clientId, privateHistory),
    presentShell: (snapshot: OrchestrationShellSnapshot, clientId?: string) =>
      presentShell(snapshot, clientId, privateHistory),
  };
});

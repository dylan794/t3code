import type {
  OrchestrationShellSnapshot,
  OrchestrationThread,
  OrchestrationThreadShell,
  ThreadId,
} from "@t3tools/contracts";
import { mayPresentJarvisOwner } from "./JarvisOwnerCore.ts";
import { ownerPrivateChannel } from "./OwnerPrivateChannel.ts";
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
): Promise<boolean> {
  return (
    !privateThread(thread) || (!!clientId && (await mayPresentJarvisOwner(thread.id, clientId)))
  );
}
export async function presentThread(
  thread: OrchestrationThread,
  clientId?: string,
): Promise<OrchestrationThread> {
  if (await permittedThread(thread, clientId)) return thread;
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
export async function presentShellThread(
  thread: OrchestrationThreadShell,
  clientId?: string,
): Promise<OrchestrationThreadShell> {
  if (await permittedThread(thread, clientId)) return thread;
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
): Promise<OrchestrationShellSnapshot> {
  return {
    ...snapshot,
    threads: await Promise.all(
      snapshot.threads.map((thread) => presentShellThread(thread, clientId)),
    ),
  };
}

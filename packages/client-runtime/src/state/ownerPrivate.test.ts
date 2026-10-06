import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  ProjectId,
  ProviderInstanceId,
  type OrchestrationShellSnapshot,
} from "@t3tools/contracts";
import {
  isOwnerPrivateConcealed,
  isOwnerPrivateVolatile,
  ownerPrivatePresentation,
  receiveOwnerPrivateFrame,
  revokeOwnerPrivatePresentation,
  dismissOwnerPrivatePresentation,
  redactOwnerPrivateShell,
} from "./ownerPrivate.ts";

describe("private Owner presentation", () => {
  it("returns to a safe shell while keeping private history locked against late frames", () => {
    const environmentId = EnvironmentId.make("privacy-recovery");
    const threadId = ThreadId.make("private-recovery-thread");
    const frame = { threadId, requestId: "recovery-request", epoch: 4 };
    receiveOwnerPrivateFrame(environmentId, { ...frame, kind: "conceal" });
    dismissOwnerPrivatePresentation({ ...frame, kind: "conceal", environmentId });
    expect(ownerPrivatePresentation.getSnapshot()).toBe(null);
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    receiveOwnerPrivateFrame(environmentId, { ...frame, kind: "restore" });
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    const thread = {
      id: threadId,
      projectId: ProjectId.make("synthetic-project"),
      title: "PRIVATE-TITLE",
      modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: "synthetic" },
      runtimeMode: "full-access" as const,
      interactionMode: "default" as const,
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
    const snapshot: OrchestrationShellSnapshot = {
      snapshotSequence: 1,
      updatedAt: thread.updatedAt,
      projects: [],
      threads: [
        thread,
        {
          ...thread,
          id: ThreadId.make("ordinary-recovery-thread"),
          title: "Ordinary work",
          branch: null,
          worktreePath: null,
        },
      ],
    };
    const presented = redactOwnerPrivateShell(environmentId, snapshot);
    expect(JSON.stringify(presented)).not.toContain("PRIVATE-");
    expect(presented.threads[1]?.title).toBe("Ordinary work");
    expect(isOwnerPrivateVolatile(environmentId)).toBe(true);
    receiveOwnerPrivateFrame(environmentId, {
      ...frame,
      epoch: 5,
      requestId: "fresh-request",
      kind: "conceal",
    });
    expect(ownerPrivatePresentation.getSnapshot()?.requestId).toBe("fresh-request");
    receiveOwnerPrivateFrame(environmentId, {
      ...frame,
      epoch: 5,
      requestId: "fresh-request",
      kind: "restore",
    });
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(false);
  });
  it("does not dismiss a newer private request when an older leave completes", () => {
    const environmentId = EnvironmentId.make("privacy-leave-race");
    const threadId = ThreadId.make("private-leave-race");
    const oldFrame = {
      environmentId,
      threadId,
      requestId: "old-leave",
      epoch: 4,
      kind: "cancel" as const,
    };
    receiveOwnerPrivateFrame(environmentId, oldFrame);
    const fresh = {
      ...oldFrame,
      requestId: "new-authentication",
      epoch: 5,
      kind: "conceal" as const,
    };
    receiveOwnerPrivateFrame(environmentId, fresh);
    dismissOwnerPrivatePresentation(oldFrame);
    expect(ownerPrivatePresentation.getSnapshot()).toEqual(fresh);
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    receiveOwnerPrivateFrame(environmentId, { ...fresh, kind: "restore" });
  });
  it("keeps cancellation and stale restore locked, and restored content volatile", () => {
    const environmentId = EnvironmentId.make("privacy-one");
    const threadId = ThreadId.make("private-thread");
    const frame = { threadId, requestId: "request", epoch: 5 };
    receiveOwnerPrivateFrame(environmentId, { ...frame, kind: "conceal" });
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    receiveOwnerPrivateFrame(environmentId, { ...frame, kind: "cancel" });
    receiveOwnerPrivateFrame(environmentId, { ...frame, epoch: 4, kind: "restore" });
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    receiveOwnerPrivateFrame(environmentId, { ...frame, kind: "restore" });
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(false);
    expect(isOwnerPrivateVolatile(environmentId, threadId)).toBe(true);
    expect(ownerPrivatePresentation.getSnapshot()).toBe(null);
    revokeOwnerPrivatePresentation(environmentId);
    expect(isOwnerPrivateConcealed(environmentId, threadId)).toBe(true);
    expect(ownerPrivatePresentation.getSnapshot()?.kind).toBe("cancel");
  });
  it("rejects restore without a matching private request", () => {
    const environmentId = EnvironmentId.make("privacy-two");
    const threadId = ThreadId.make("untouched-thread");
    receiveOwnerPrivateFrame(environmentId, {
      threadId,
      requestId: "unknown",
      epoch: 2,
      kind: "restore",
    });
    expect(isOwnerPrivateVolatile(environmentId, threadId)).toBe(false);
  });
});

import {
  ThreadId,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type OrchestrationThreadDetailSnapshot,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  permittedThread,
  presentThread,
  presentShellThread,
  presentThreadSnapshot,
} from "./OwnerThreadPresentation.ts";

describe("private thread presentation", () => {
  it("retains the bounded activity projection when presenting an ordinary HTTP or WebSocket snapshot", async () => {
    const snapshot = {
      thread: {
        id: ThreadId.make("ordinary-snapshot"),
        session: { providerName: "codex" },
        activities: [
          {
            kind: "tool.completed",
            summary: "Command completed",
            turnId: null,
            payload: {
              itemType: "command_execution",
              data: {
                item: {
                  command: "echo hello",
                  aggregatedOutput: `hello\n${"PRIVATE-LARGE-BODY".repeat(1000)}`,
                },
              },
            },
          },
        ],
      },
    } as unknown as OrchestrationThreadDetailSnapshot;
    const result = await presentThreadSnapshot(snapshot);
    expect(JSON.stringify(result)).not.toContain("PRIVATE-LARGE-BODY");
    expect(result.thread.activities[0]?.payload).toMatchObject({
      data: { item: { command: "echo hello", aggregatedOutput: "hello" } },
    });
    expect(JSON.stringify(snapshot)).toContain("PRIVATE-LARGE-BODY");
  });
  it("redacts stopped Pi history even when the in-process broker has never observed it", async () => {
    const shell = {
      id: ThreadId.make("persisted-pi-thread"),
      title: "PRIVATE-TITLE",
      branch: "PRIVATE-BRANCH",
      worktreePath: "PRIVATE-PATH",
      session: { providerName: "pi" },
      latestTurn: { assistantMessageId: "PRIVATE-ID" },
      latestUserMessageAt: "PRIVATE-DATE",
      hasPendingApprovals: true,
      hasPendingUserInput: true,
      hasActionableProposedPlan: true,
      titleRegeneration: "PRIVATE",
      planProgress: "PRIVATE",
    } as unknown as OrchestrationThreadShell;
    expect(await permittedThread(shell)).toBe(false);
    const redacted = await presentShellThread(shell);
    expect(JSON.stringify(redacted)).not.toContain("PRIVATE");
    const thread = {
      id: shell.id,
      title: shell.title,
      branch: shell.branch,
      worktreePath: shell.worktreePath,
      session: shell.session,
      latestTurn: shell.latestTurn,
      titleRegeneration: shell.titleRegeneration,
      messages: [{ text: "PRIVATE-BODY" }],
      activities: [{ payload: "PRIVATE-TOOL" }],
      checkpoints: [{ files: "PRIVATE-FILES" }],
      proposedPlans: [{ planMarkdown: "PRIVATE-PLAN" }],
    } as unknown as OrchestrationThread;
    expect(JSON.stringify(await presentThread(thread))).not.toContain("PRIVATE");
    expect(thread.messages).toHaveLength(1);
  });
  it("leaves unrelated provider transcripts unchanged", async () => {
    const thread = {
      id: ThreadId.make("ordinary-thread"),
      session: { providerName: "codex" },
    } as OrchestrationThread;
    expect(await presentThread(thread)).toBe(thread);
  });
});

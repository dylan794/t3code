import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  isOwnerPrivateConcealed,
  isOwnerPrivateVolatile,
  ownerPrivatePresentation,
  receiveOwnerPrivateFrame,
  revokeOwnerPrivatePresentation,
} from "./ownerPrivate.ts";

describe("private Owner presentation", () => {
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

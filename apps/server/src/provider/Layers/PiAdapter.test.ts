// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings, ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { makePiAdapter } from "./PiAdapter.ts";

const fixturePath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "..",
  "pi",
  "testFixtures",
  "piRpcMockPeer.mjs",
);
const decodeSettings = Schema.decodeSync(PiSettings);

function makeFakeJarvisRoot(): string {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-adapter-"));
  const cli = NodePath.join(
    root,
    "node_modules",
    "@earendil-works",
    "pi-coding-agent",
    "dist",
    "cli.js",
  );
  const extension = NodePath.join(root, "src", "extension.ts");
  NodeFS.mkdirSync(NodePath.dirname(cli), { recursive: true });
  NodeFS.mkdirSync(NodePath.dirname(extension), { recursive: true });
  NodeFS.copyFileSync(fixturePath, cli);
  NodeFS.writeFileSync(extension, "export default function jarvis() {}\n");
  return root;
}

describe("PiAdapter", () => {
  it.effect("starts Jarvis, streams a turn, and interrupts only its scoped Pi process", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const adapter = yield* makePiAdapter(decodeSettings({ jarvisProjectPath: jarvisRoot }), {
        instanceId: ProviderInstanceId.make("pi-test"),
      });
      const threadId = ThreadId.make("pi-thread");
      const firstEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        providerInstanceId: ProviderInstanceId.make("pi-test"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });
      const firstTurn = yield* adapter.sendTurn({ threadId, input: "hello" });
      const firstEvents = Array.from(yield* Fiber.join(firstEventsFiber));

      expect(session.resumeCursor).toEqual({
        schemaVersion: 1,
        sessionFile: "mock-session.jsonl",
        sessionId: "mock-session",
      });
      expect(firstEvents.map((event) => event.type)).toContain("content.delta");
      expect(firstEvents.at(-1)).toMatchObject({
        type: "turn.completed",
        turnId: firstTurn.turnId,
      });

      const interruptEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.aborted"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const secondTurn = yield* adapter.sendTurn({ threadId, input: "wait" });
      yield* adapter.interruptTurn(threadId, secondTurn.turnId);
      const interruptEvents = Array.from(yield* Fiber.join(interruptEventsFiber));
      expect(interruptEvents.at(-1)).toMatchObject({
        type: "turn.aborted",
        turnId: secondTurn.turnId,
      });
      expect(yield* adapter.hasSession(threadId)).toBe(true);
      yield* adapter.stopSession(threadId);
      expect(yield* adapter.hasSession(threadId)).toBe(false);

      const exitEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "session.exited"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "exit" });
      const exitEvents = Array.from(yield* Fiber.join(exitEventsFiber));
      expect(exitEvents.at(-1)).toMatchObject({
        type: "session.exited",
        payload: { exitKind: "error", recoverable: true },
      });
      expect(yield* adapter.hasSession(threadId)).toBe(false);
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

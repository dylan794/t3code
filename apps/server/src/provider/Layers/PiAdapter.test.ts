// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
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

  it.effect("maps Pi confirmation requests and rejects session-persistent approval", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const adapter = yield* makePiAdapter(decodeSettings({ jarvisProjectPath: jarvisRoot }), {
        instanceId: ProviderInstanceId.make("pi-test"),
      });
      const threadId = ThreadId.make("pi-confirm-thread");
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });

      for (const [decision, expectedDelta] of [
        ["accept", "confirmed"],
        ["decline", "declined"],
        ["cancel", "cancelled"],
      ] as const) {
        const openedFiber = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "request.opened"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const turn = yield* adapter.sendTurn({ threadId, input: "confirm" });
        const openedEvents = Array.from(yield* Fiber.join(openedFiber));
        expect(openedEvents.at(-1)).toMatchObject({
          type: "request.opened",
          turnId: turn.turnId,
          requestId: "mock-confirm",
          payload: {
            requestType: "command_execution_approval",
            supportsAcceptForSession: false,
            detail: "Run command?\nnpm test",
          },
        });

        if (decision === "accept") {
          const unsupported = yield* Effect.flip(
            adapter.respondToRequest(
              threadId,
              ApprovalRequestId.make("mock-confirm"),
              "acceptForSession",
            ),
          );
          expect(unsupported).toMatchObject({
            _tag: "ProviderAdapterRequestError",
            detail: "Pi confirmation requests do not support session-persistent approval.",
          });
        }

        const completedFiber = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* adapter.respondToRequest(threadId, ApprovalRequestId.make("mock-confirm"), decision);
        const completedEvents = Array.from(yield* Fiber.join(completedFiber));
        expect(completedEvents).toContainEqual(
          expect.objectContaining({
            type: "request.resolved",
            requestId: "mock-confirm",
            payload: expect.objectContaining({ decision }),
          }),
        );
        expect(completedEvents).toContainEqual(
          expect.objectContaining({
            type: "content.delta",
            payload: expect.objectContaining({ delta: expectedDelta }),
          }),
        );
      }

      const zeroTimeoutOpenedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "request.opened"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId, input: "timeout-zero" });
      yield* Fiber.join(zeroTimeoutOpenedFiber);
      const zeroTimeoutCompletedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make("mock-timeout-zero"),
        "accept",
      );
      yield* Fiber.join(zeroTimeoutCompletedFiber);

      const stale = yield* Effect.flip(
        adapter.respondToRequest(threadId, ApprovalRequestId.make("mock-confirm"), "accept"),
      );
      expect(stale).toMatchObject({
        _tag: "ProviderAdapterRequestError",
        detail: "Unknown pending approval request: mock-confirm",
      });
      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("maps Pi input, select, and editor dialogs to structured user input", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const adapter = yield* makePiAdapter(decodeSettings({ jarvisProjectPath: jarvisRoot }), {
        instanceId: ProviderInstanceId.make("pi-test"),
      });
      const threadId = ThreadId.make("pi-input-thread");
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });

      const cases = [
        {
          prompt: "input",
          requestId: "mock-input",
          value: "   ",
          question: {
            header: "Input",
            question: "Your name",
            options: [],
            placeholder: "Type a name",
            inputKind: "text",
          },
          delta: "input:   ",
        },
        {
          prompt: "select",
          requestId: "mock-select",
          value: " War Machine ",
          question: {
            header: "Selection",
            question: "Choose a suit",
            options: [
              { label: "Iron Man", description: "Iron Man", value: "Iron Man" },
              {
                label: '" War Machine "',
                description: 'Exact value: " War Machine "',
                value: " War Machine ",
              },
            ],
            inputKind: "text",
          },
          delta: "select: War Machine ",
        },
        {
          prompt: "editor",
          requestId: "mock-editor",
          value: "",
          question: {
            header: "Editor",
            question: "Edit briefing",
            options: [],
            defaultValue: "Keep the whitespace\n",
            inputKind: "multiline",
          },
          delta: "editor:",
        },
      ] as const;

      for (const testCase of cases) {
        const requestedFiber = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "user-input.requested"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* adapter.sendTurn({ threadId, input: testCase.prompt });
        const requestedEvents = Array.from(yield* Fiber.join(requestedFiber));
        expect(requestedEvents.at(-1)).toMatchObject({
          type: "user-input.requested",
          requestId: testCase.requestId,
          payload: { questions: [expect.objectContaining(testCase.question)] },
        });

        if (testCase.prompt === "select") {
          const invalid = yield* Effect.flip(
            adapter.respondToUserInput(threadId, ApprovalRequestId.make(testCase.requestId), {
              value: "Hulkbuster",
            }),
          );
          expect(invalid).toMatchObject({
            _tag: "ProviderAdapterRequestError",
            detail: expect.stringContaining("must match one of the offered options"),
          });
        }

        const completedFiber = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make(testCase.requestId), {
          value: testCase.value,
        });
        const completedEvents = Array.from(yield* Fiber.join(completedFiber));
        expect(completedEvents).toContainEqual(
          expect.objectContaining({
            type: "user-input.resolved",
            requestId: testCase.requestId,
            payload: { answers: { value: testCase.value } },
          }),
        );
        expect(completedEvents).toContainEqual(
          expect.objectContaining({
            type: "content.delta",
            payload: expect.objectContaining({ delta: testCase.delta }),
          }),
        );
      }

      const cancelRequestedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "user-input.requested"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId, input: "input" });
      yield* Fiber.join(cancelRequestedFiber);
      const cancelCompletedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("mock-input"), {}, true);
      const cancelEvents = Array.from(yield* Fiber.join(cancelCompletedFiber));
      expect(cancelEvents).toContainEqual(
        expect.objectContaining({
          type: "user-input.resolved",
          requestId: "mock-input",
          payload: { answers: {}, cancelled: true },
        }),
      );
      expect(cancelEvents).toContainEqual(
        expect.objectContaining({
          type: "content.delta",
          payload: expect.objectContaining({ delta: "cancelled" }),
        }),
      );

      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("rolls Pi back by forking before the removed user turn", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const adapter = yield* makePiAdapter(decodeSettings({ jarvisProjectPath: jarvisRoot }), {
        instanceId: ProviderInstanceId.make("pi-test"),
      });
      const threadId = ThreadId.make("pi-rollback-thread");
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });

      for (const input of ["first", "second"]) {
        const completedFiber = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* adapter.sendTurn({ threadId, input });
        yield* Fiber.join(completedFiber);
      }

      expect((yield* adapter.readThread(threadId)).turns).toHaveLength(2);
      const invalid = yield* Effect.flip(adapter.rollbackThread(threadId, 0));
      expect(invalid).toMatchObject({
        _tag: "ProviderAdapterValidationError",
        issue: "numTurns must be an integer >= 1.",
      });

      const firstRollback = yield* adapter.rollbackThread(threadId, 1);
      expect(firstRollback.turns).toHaveLength(1);
      expect(firstRollback.resumeCursor).toEqual({
        schemaVersion: 1,
        sessionFile: "mock-session-fork-1.jsonl",
        sessionId: "mock-session-fork-1",
      });
      expect((yield* adapter.listSessions())[0]?.resumeCursor).toEqual(firstRollback.resumeCursor);

      const secondRollback = yield* adapter.rollbackThread(threadId, 99);
      expect(secondRollback.turns).toHaveLength(0);
      expect(secondRollback.resumeCursor).toEqual({
        schemaVersion: 1,
        sessionFile: "mock-session-fork-2.jsonl",
        sessionId: "mock-session-fork-2",
      });

      yield* adapter.stopSession(threadId);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("clears timed-out and stopped Pi interactions from the durable UI", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const adapter = yield* makePiAdapter(decodeSettings({ jarvisProjectPath: jarvisRoot }), {
        instanceId: ProviderInstanceId.make("pi-test"),
      });
      const threadId = ThreadId.make("pi-cleanup-thread");
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("pi"),
        cwd: jarvisRoot,
        runtimeMode: "full-access",
      });

      const timeoutFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId, input: "timeout" });
      const timeoutEvents = Array.from(yield* Fiber.join(timeoutFiber));
      expect(timeoutEvents).toContainEqual(
        expect.objectContaining({
          type: "request.resolved",
          requestId: "mock-timeout",
          payload: expect.objectContaining({ decision: "decline" }),
        }),
      );

      const invalidFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId, input: "invalid-dialog" });
      const invalidEvents = Array.from(yield* Fiber.join(invalidFiber));
      expect(invalidEvents).toContainEqual(
        expect.objectContaining({
          type: "runtime.error",
          payload: expect.objectContaining({ class: "validation_error" }),
        }),
      );
      expect(invalidEvents).toContainEqual(
        expect.objectContaining({
          type: "content.delta",
          payload: expect.objectContaining({ delta: "cancelled" }),
        }),
      );

      const requestedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "user-input.requested"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const interruptTurn = yield* adapter.sendTurn({ threadId, input: "input" });
      yield* Fiber.join(requestedFiber);

      const interruptedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.aborted"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.interruptTurn(threadId, interruptTurn.turnId);
      const interruptedEvents = Array.from(yield* Fiber.join(interruptedFiber));
      expect(interruptedEvents).toContainEqual(
        expect.objectContaining({
          type: "user-input.resolved",
          requestId: "mock-input",
          payload: { answers: {}, cancelled: true },
        }),
      );
      expect(interruptedEvents).toContainEqual(
        expect.objectContaining({ type: "turn.aborted", turnId: interruptTurn.turnId }),
      );

      const stopRequestedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "user-input.requested"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.sendTurn({ threadId, input: "input" });
      yield* Fiber.join(stopRequestedFiber);

      const stoppedFiber = yield* adapter.streamEvents.pipe(
        Stream.takeUntil(
          (event) => event.type === "user-input.resolved" && event.requestId === "mock-input",
        ),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* adapter.stopSession(threadId);
      const stoppedEvents = Array.from(yield* Fiber.join(stoppedFiber));
      expect(stoppedEvents.at(-1)).toMatchObject({
        type: "user-input.resolved",
        requestId: "mock-input",
        payload: { answers: {}, cancelled: true },
      });
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
      yield* adapter.sendTurn({ threadId, input: "exit-with-input" });
      const exitEvents = Array.from(yield* Fiber.join(exitEventsFiber));
      expect(exitEvents).toContainEqual(
        expect.objectContaining({
          type: "user-input.resolved",
          requestId: "mock-exit-input",
          payload: { answers: {}, cancelled: true },
        }),
      );
      expect(exitEvents.at(-1)).toMatchObject({ type: "session.exited" });
      expect(yield* adapter.hasSession(threadId)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

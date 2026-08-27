// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import {
  buildPiRpcLaunchArgs,
  makePiRpcConnection,
  type PiRpcTransportError,
  splitPiRpcStdoutChunk,
} from "./PiRpcTransport.ts";
import type { PiRpcEvent } from "./PiRpcProtocol.ts";

const mockPeerPath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "testFixtures",
  "piRpcMockPeer.mjs",
);

describe("PiRpcTransport", () => {
  it("launches Pi RPC with only the Jarvis extension", () => {
    expect(
      buildPiRpcLaunchArgs({
        nodePath: "C:/node/node.exe",
        piCliPath: "C:/jarvis/node_modules/pi/dist/cli.js",
        extensionPath: "C:/jarvis/src/extension.ts",
        cwd: "C:/jarvis",
      }),
    ).toEqual([
      "C:/jarvis/node_modules/pi/dist/cli.js",
      "--mode",
      "rpc",
      "--no-extensions",
      "--extension",
      "C:/jarvis/src/extension.ts",
    ]);
  });

  it("resumes an existing Pi session and preserves explicit model arguments", () => {
    expect(
      buildPiRpcLaunchArgs({
        nodePath: "node",
        piCliPath: "pi.js",
        extensionPath: "extension.ts",
        cwd: "C:/jarvis",
        sessionPath: "C:/sessions/thread.jsonl",
        additionalArgs: ["--provider", "openai-codex", "--model", "gpt-5.4"],
      }),
    ).toEqual([
      "pi.js",
      "--mode",
      "rpc",
      "--no-extensions",
      "--extension",
      "extension.ts",
      "--session",
      "C:/sessions/thread.jsonl",
      "--provider",
      "openai-codex",
      "--model",
      "gpt-5.4",
    ]);
  });

  it("uses LF-only framing and strips only a CR immediately before LF", () => {
    const first = splitPiRpcStdoutChunk(
      "",
      '{"type":"message_update","text":"a\rb\u2028c\u2029d"}\r\n{"type":"agent_',
    );
    expect(first).toEqual({
      lines: ['{"type":"message_update","text":"a\rb\u2028c\u2029d"}'],
      remainder: '{"type":"agent_',
    });

    expect(splitPiRpcStdoutChunk(first.remainder, 'settled"}\n')).toEqual({
      lines: ['{"type":"agent_settled"}'],
      remainder: "",
    });
  });

  it.effect("correlates commands and streams a complete Pi run", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath: mockPeerPath,
        extensionPath: "ignored-by-mock",
        cwd: NodePath.dirname(mockPeerPath),
      });

      const eventsFiber = yield* connection.events.pipe(
        Stream.takeUntil((event) => event.type === "run.settled"),
        Stream.runCollect,
        Effect.forkChild,
      );
      const state = yield* connection.getState();
      yield* connection.prompt("hello");
      const events = Array.from(yield* Fiber.join(eventsFiber));
      yield* connection.abort();

      expect(state).toEqual({
        sessionId: "mock-session",
        sessionFile: "mock-session.jsonl",
        isStreaming: false,
      });
      expect(events).toEqual([
        { type: "run.started" },
        { type: "assistant.started" },
        {
          type: "assistant.delta",
          stream: "text",
          contentIndex: 0,
          delta: "mock reply",
        },
        { type: "assistant.completed", stopReason: "stop" },
        { type: "run.ended", willRetry: false },
        { type: "run.settled" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("records whether a notification preceded its prompt response", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath: mockPeerPath,
        extensionPath: "ignored-by-mock",
        cwd: NodePath.dirname(mockPeerPath),
      });

      const duringFiber = yield* connection.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* connection.prompt("handled-without-run-info");
      expect(Array.from(yield* Fiber.join(duringFiber))).toEqual([
        expect.objectContaining({
          type: "extension-ui.notified",
          observedDuringPromptRequest: true,
        }),
      ]);

      const afterFiber = yield* connection.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* connection.prompt("handled-then-notified-delayed-run");
      expect(Array.from(yield* Fiber.join(afterFiber))).toEqual([
        expect.objectContaining({
          type: "extension-ui.notified",
          observedDuringPromptRequest: false,
        }),
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("serializes concurrent writes and surfaces rejected commands", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath: mockPeerPath,
        extensionPath: "ignored-by-mock",
        cwd: NodePath.dirname(mockPeerPath),
      });

      const responses = yield* Effect.all(
        [connection.request({ type: "get_state" }), connection.request({ type: "get_state" })],
        { concurrency: "unbounded" },
      );
      const rejection = yield* Effect.flip(
        connection.request({ type: "set_session_name", name: "rejected" }),
      );

      expect(responses.map((response) => response.id)).toEqual(["t3-pi-1", "t3-pi-2"]);
      expect(rejection).toMatchObject({
        _tag: "PiRpcTransportError",
        operation: "response",
        detail: "mock rejected session name",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("writes confirmation responses without waiting for a command response", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath: mockPeerPath,
        extensionPath: "ignored-by-mock",
        cwd: NodePath.dirname(mockPeerPath),
      });

      const eventsFiber = yield* connection.events.pipe(
        Stream.takeUntil((event) => event.type === "run.settled"),
        Stream.mapEffect(
          (event): Effect.Effect<PiRpcEvent, PiRpcTransportError> =>
            event.type === "extension-ui.requested" && event.method === "confirm"
              ? Effect.all(
                  [
                    connection.respondToExtensionUI({
                      type: "extension_ui_response",
                      id: event.requestId,
                      confirmed: true,
                    }),
                    connection.request({ type: "get_state" }),
                  ],
                  { concurrency: "unbounded" },
                ).pipe(Effect.as(event))
              : Effect.succeed(event),
        ),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* connection.prompt("confirm");
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect(events).toContainEqual({
        type: "extension-ui.requested",
        requestId: "mock-confirm",
        method: "confirm",
        title: "Run command?",
        message: "npm test",
        timeout: 10_000,
      });
      expect(events).toContainEqual({
        type: "assistant.delta",
        stream: "text",
        contentIndex: 0,
        delta: "confirmed",
      });

      const cancelledEventsFiber = yield* connection.events.pipe(
        Stream.takeUntil((event) => event.type === "run.settled"),
        Stream.mapEffect(
          (event): Effect.Effect<PiRpcEvent, PiRpcTransportError> =>
            event.type === "extension-ui.requested" && event.method === "confirm"
              ? connection
                  .respondToExtensionUI({
                    type: "extension_ui_response",
                    id: event.requestId,
                    cancelled: true,
                  })
                  .pipe(Effect.as(event))
              : Effect.succeed(event),
        ),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* connection.prompt("confirm");
      const cancelledEvents = Array.from(yield* Fiber.join(cancelledEventsFiber));
      expect(cancelledEvents).toContainEqual({
        type: "assistant.delta",
        stream: "text",
        contentIndex: 0,
        delta: "cancelled",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("writes value responses for extension text input", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath: mockPeerPath,
        extensionPath: "ignored-by-mock",
        cwd: NodePath.dirname(mockPeerPath),
      });

      const eventsFiber = yield* connection.events.pipe(
        Stream.takeUntil((event) => event.type === "run.settled"),
        Stream.mapEffect(
          (event): Effect.Effect<PiRpcEvent, PiRpcTransportError> =>
            event.type === "extension-ui.requested" && event.method === "input"
              ? connection
                  .respondToExtensionUI({
                    type: "extension_ui_response",
                    id: event.requestId,
                    value: "Dylan",
                  })
                  .pipe(Effect.as(event))
              : Effect.succeed(event),
        ),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* connection.prompt("input");
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect(events).toContainEqual({
        type: "extension-ui.requested",
        requestId: "mock-input",
        method: "input",
        title: "Your name",
        placeholder: "Type a name",
      });
      expect(events).toContainEqual({
        type: "assistant.delta",
        stream: "text",
        contentIndex: 0,
        delta: "input:Dylan",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

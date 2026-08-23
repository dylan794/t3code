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
  splitPiRpcStdoutChunk,
} from "./PiRpcTransport.ts";

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

      expect(state).toEqual({ sessionId: "mock-session", sessionFile: "mock-session.jsonl" });
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
});

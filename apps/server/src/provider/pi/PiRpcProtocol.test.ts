import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { describe, expect } from "vite-plus/test";

import {
  decodePiRpcLine,
  normalizePiRpcEvent,
  responseFromPiWireMessage,
} from "./PiRpcProtocol.ts";

const decode = decodePiRpcLine;

describe("PiRpcProtocol", () => {
  it.effect("decodes correlated command responses", () =>
    Effect.gen(function* () {
      const message = yield* decode(
        '{"id":"t3-pi-1","type":"response","command":"prompt","success":true}',
      );

      expect(responseFromPiWireMessage(message)).toEqual({
        _tag: "success",
        response: { id: "t3-pi-1", command: "prompt" },
      });
    }),
  );

  it.effect("normalizes assistant text and thinking deltas", () =>
    Effect.gen(function* () {
      const text = yield* decode(
        '{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hello"}}',
      );
      const thinking = yield* decode(
        '{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":1,"delta":"Check"}}',
      );

      expect(normalizePiRpcEvent(text)).toEqual([
        { type: "assistant.delta", stream: "text", contentIndex: 0, delta: "Hello" },
      ]);
      expect(normalizePiRpcEvent(thinking)).toEqual([
        { type: "assistant.delta", stream: "thinking", contentIndex: 1, delta: "Check" },
      ]);
    }),
  );

  it.effect("uses agent settlement as the full-run completion signal", () =>
    Effect.gen(function* () {
      const started = yield* decode('{"type":"agent_start"}');
      const endedForRetry = yield* decode('{"type":"agent_end","messages":[],"willRetry":true}');
      const settled = yield* decode('{"type":"agent_settled"}');

      expect(normalizePiRpcEvent(started)).toEqual([{ type: "run.started" }]);
      expect(normalizePiRpcEvent(endedForRetry)).toEqual([{ type: "run.ended", willRetry: true }]);
      expect(normalizePiRpcEvent(settled)).toEqual([{ type: "run.settled" }]);
    }),
  );

  it.effect("normalizes tool lifecycle events and accumulated output", () =>
    Effect.gen(function* () {
      const started = yield* decode(
        '{"type":"tool_execution_start","toolCallId":"call-1","toolName":"bash","args":{"command":"git status"}}',
      );
      const updated = yield* decode(
        '{"type":"tool_execution_update","toolCallId":"call-1","toolName":"bash","args":{"command":"git status"},"partialResult":{"content":[{"type":"text","text":"M file.ts"}]}}',
      );
      const completed = yield* decode(
        '{"type":"tool_execution_end","toolCallId":"call-1","toolName":"bash","result":{"content":[{"type":"text","text":"clean"}]},"isError":false}',
      );

      expect(normalizePiRpcEvent(started)).toEqual([
        {
          type: "tool.started",
          toolCallId: "call-1",
          toolName: "bash",
          itemType: "command_execution",
          args: { command: "git status" },
        },
      ]);
      expect(normalizePiRpcEvent(updated)).toEqual([
        {
          type: "tool.updated",
          toolCallId: "call-1",
          toolName: "bash",
          itemType: "command_execution",
          detail: "M file.ts",
          data: { content: [{ type: "text", text: "M file.ts" }] },
        },
      ]);
      expect(normalizePiRpcEvent(completed)).toEqual([
        {
          type: "tool.completed",
          toolCallId: "call-1",
          toolName: "bash",
          itemType: "command_execution",
          failed: false,
          detail: "clean",
          data: { content: [{ type: "text", text: "clean" }] },
        },
      ]);
    }),
  );

  it.effect("rejects invalid JSON without throwing a defect", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(decodePiRpcLine("not-json"));
      expect(error).toMatchObject({
        _tag: "PiRpcDecodeError",
        line: "not-json",
      });
    }),
  );
});

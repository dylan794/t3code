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

  it.effect("normalizes extension UI dialog requests", () =>
    Effect.gen(function* () {
      const select = yield* decode(
        '{"type":"extension_ui_request","id":"select-1","method":"select","title":"Choose","options":["One","Two"],"timeout":5000}',
      );
      const confirm = yield* decode(
        '{"type":"extension_ui_request","id":"confirm-1","method":"confirm","title":"Run command?","message":"npm test","timeout":10000}',
      );
      const input = yield* decode(
        '{"type":"extension_ui_request","id":"input-1","method":"input","title":"Your name","placeholder":"Type a name","timeout":15000}',
      );
      const editor = yield* decode(
        '{"type":"extension_ui_request","id":"editor-1","method":"editor","title":"Edit plan","prefill":"Line 1\\nLine 2"}',
      );

      expect(normalizePiRpcEvent(select)).toEqual([
        {
          type: "extension-ui.requested",
          requestId: "select-1",
          method: "select",
          title: "Choose",
          options: ["One", "Two"],
          timeout: 5000,
        },
      ]);
      expect(normalizePiRpcEvent(confirm)).toEqual([
        {
          type: "extension-ui.requested",
          requestId: "confirm-1",
          method: "confirm",
          title: "Run command?",
          message: "npm test",
          timeout: 10000,
        },
      ]);
      expect(normalizePiRpcEvent(input)).toEqual([
        {
          type: "extension-ui.requested",
          requestId: "input-1",
          method: "input",
          title: "Your name",
          placeholder: "Type a name",
          timeout: 15000,
        },
      ]);
      expect(normalizePiRpcEvent(editor)).toEqual([
        {
          type: "extension-ui.requested",
          requestId: "editor-1",
          method: "editor",
          title: "Edit plan",
          prefill: "Line 1\nLine 2",
        },
      ]);
    }),
  );

  it.effect("reports incomplete extension UI dialogs and invalid field types", () =>
    Effect.gen(function* () {
      const missingTitle = yield* decode(
        '{"type":"extension_ui_request","id":"confirm-1","method":"confirm","message":"npm test"}',
      );
      expect(normalizePiRpcEvent(missingTitle)).toEqual([
        {
          type: "extension-ui.invalid",
          requestId: "confirm-1",
          message: "Pi emitted an invalid extension UI dialog request.",
        },
      ]);

      const invalidOptions = yield* decodePiRpcLine(
        '{"type":"extension_ui_request","id":"select-1","method":"select","title":"Choose","options":[1]}',
      );
      expect(normalizePiRpcEvent(invalidOptions)).toEqual([
        {
          type: "extension-ui.invalid",
          requestId: "select-1",
          message: "Pi emitted an invalid extension UI dialog request.",
        },
      ]);

      const notification = yield* decode(
        '{"type":"extension_ui_request","id":"notify-1","method":"notify","message":"Authentication blocked","notifyType":"warning"}',
      );
      expect(normalizePiRpcEvent(notification)).toEqual([
        {
          type: "extension-ui.notified",
          requestId: "notify-1",
          message: "Authentication blocked",
          level: "warning",
        },
      ]);
    }),
  );

  it.effect("normalizes fire-and-forget editor text requests", () =>
    Effect.gen(function* () {
      const request = yield* decode(
        '{"type":"extension_ui_request","id":"editor-text-1","method":"set_editor_text","text":" Keep this whitespace\\n"}',
      );

      expect(normalizePiRpcEvent(request)).toEqual([
        {
          type: "editor-text.requested",
          requestId: "editor-text-1",
          text: " Keep this whitespace\n",
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

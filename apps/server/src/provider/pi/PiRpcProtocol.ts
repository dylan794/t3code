import type { ToolLifecycleItemType } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

const PiAssistantMessageEvent = Schema.Struct({
  type: Schema.String,
  contentIndex: Schema.optional(Schema.Int),
  delta: Schema.optional(Schema.String),
  content: Schema.optional(Schema.String),
  toolCall: Schema.optional(Schema.Unknown),
});

const PiWireMessage = Schema.Struct({
  type: Schema.String,
  id: Schema.optional(Schema.String),
  method: Schema.optional(Schema.Unknown),
  command: Schema.optional(Schema.String),
  success: Schema.optional(Schema.Boolean),
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Unknown),
  message: Schema.optional(Schema.Unknown),
  title: Schema.optional(Schema.Unknown),
  options: Schema.optional(Schema.Unknown),
  timeout: Schema.optional(Schema.Unknown),
  placeholder: Schema.optional(Schema.Unknown),
  prefill: Schema.optional(Schema.Unknown),
  text: Schema.optional(Schema.Unknown),
  messages: Schema.optional(Schema.Array(Schema.Unknown)),
  willRetry: Schema.optional(Schema.Boolean),
  usage: Schema.optional(Schema.Unknown),
  assistantMessageEvent: Schema.optional(PiAssistantMessageEvent),
  toolCallId: Schema.optional(Schema.String),
  toolName: Schema.optional(Schema.String),
  args: Schema.optional(Schema.Unknown),
  partialResult: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Unknown),
  isError: Schema.optional(Schema.Boolean),
});

export type PiWireMessage = typeof PiWireMessage.Type;

const PiMessage = Schema.Struct({
  role: Schema.String,
  stopReason: Schema.optional(Schema.NullOr(Schema.String)),
  errorMessage: Schema.optional(Schema.String),
});

const PiToolResult = Schema.Struct({
  content: Schema.optional(
    Schema.Array(
      Schema.Struct({
        type: Schema.String,
        text: Schema.optional(Schema.String),
      }),
    ),
  ),
});

const PiExtensionUIDialogRequest = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    id: Schema.String,
    method: Schema.Literal("select"),
    title: Schema.String,
    options: Schema.Array(Schema.String),
    timeout: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    id: Schema.String,
    method: Schema.Literal("confirm"),
    title: Schema.String,
    message: Schema.String,
    timeout: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    id: Schema.String,
    method: Schema.Literal("input"),
    title: Schema.String,
    placeholder: Schema.optional(Schema.String),
    timeout: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    id: Schema.String,
    method: Schema.Literal("editor"),
    title: Schema.String,
    prefill: Schema.optional(Schema.String),
  }),
]);

const PiExtensionUISetEditorTextRequest = Schema.Struct({
  type: Schema.Literal("extension_ui_request"),
  id: Schema.String,
  method: Schema.Literal("set_editor_text"),
  text: Schema.String,
});

const decodeWireLine = Schema.decodeUnknownEffect(Schema.fromJsonString(PiWireMessage));
const decodeMessageExit = Schema.decodeUnknownExit(PiMessage);
const decodeToolResultExit = Schema.decodeUnknownExit(PiToolResult);
const decodeExtensionUIDialogRequestExit = Schema.decodeUnknownExit(PiExtensionUIDialogRequest);
const decodeExtensionUISetEditorTextRequestExit = Schema.decodeUnknownExit(
  PiExtensionUISetEditorTextRequest,
);

export class PiRpcDecodeError extends Schema.TaggedErrorClass<PiRpcDecodeError>()(
  "PiRpcDecodeError",
  {
    line: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export interface PiRpcResponse {
  readonly id: string;
  readonly command: string;
  readonly data?: unknown;
}

export type PiRpcEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.ended"; readonly willRetry: boolean }
  | { readonly type: "run.settled" }
  | { readonly type: "assistant.started" }
  | {
      readonly type: "assistant.delta";
      readonly stream: "text" | "thinking";
      readonly contentIndex: number;
      readonly delta: string;
    }
  | {
      readonly type: "assistant.completed";
      readonly stopReason?: string;
      readonly errorMessage?: string;
    }
  | {
      readonly type: "tool.started";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly itemType: ToolLifecycleItemType;
      readonly args?: unknown;
    }
  | {
      readonly type: "tool.updated";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly itemType: ToolLifecycleItemType;
      readonly detail?: string;
      readonly data?: unknown;
    }
  | {
      readonly type: "tool.completed";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly itemType: ToolLifecycleItemType;
      readonly failed: boolean;
      readonly detail?: string;
      readonly data?: unknown;
    }
  | {
      readonly type: "extension-ui.requested";
      readonly requestId: string;
      readonly method: "select";
      readonly title: string;
      readonly options: ReadonlyArray<string>;
      readonly timeout?: number;
    }
  | {
      readonly type: "extension-ui.requested";
      readonly requestId: string;
      readonly method: "confirm";
      readonly title: string;
      readonly message: string;
      readonly timeout?: number;
    }
  | {
      readonly type: "extension-ui.requested";
      readonly requestId: string;
      readonly method: "input";
      readonly title: string;
      readonly placeholder?: string;
      readonly timeout?: number;
    }
  | {
      readonly type: "extension-ui.requested";
      readonly requestId: string;
      readonly method: "editor";
      readonly title: string;
      readonly prefill?: string;
    }
  | {
      readonly type: "extension-ui.invalid";
      readonly requestId?: string;
      readonly message: string;
    }
  | {
      readonly type: "editor-text.requested";
      readonly requestId: string;
      readonly text: string;
    }
  | { readonly type: "runtime.error"; readonly message: string }
  | { readonly type: "runtime.exited"; readonly message: string };

export const decodePiRpcLine = Effect.fn("decodePiRpcLine")(function* (
  line: string,
): Effect.fn.Return<PiWireMessage, PiRpcDecodeError> {
  return yield* decodeWireLine(line).pipe(
    Effect.mapError((cause) => new PiRpcDecodeError({ line, cause })),
  );
});

export function responseFromPiWireMessage(message: PiWireMessage):
  | { readonly _tag: "success"; readonly response: PiRpcResponse }
  | {
      readonly _tag: "failure";
      readonly id: string;
      readonly command: string;
      readonly error: string;
    }
  | undefined {
  if (message.type !== "response" || !message.id || !message.command) {
    return undefined;
  }
  if (message.success === true) {
    return {
      _tag: "success",
      response: {
        id: message.id,
        command: message.command,
        ...(message.data !== undefined ? { data: message.data } : {}),
      },
    };
  }
  if (message.success === false) {
    const error = typeof message.error === "string" ? message.error.trim() : "";
    return {
      _tag: "failure",
      id: message.id,
      command: message.command,
      error: error || `Pi rejected ${message.command}`,
    };
  }
  return undefined;
}

export function normalizePiRpcEvent(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  switch (message.type) {
    case "agent_start":
      return [{ type: "run.started" }];
    case "agent_end":
      return [{ type: "run.ended", willRetry: message.willRetry === true }];
    case "agent_settled":
      return [{ type: "run.settled" }];
    case "message_start":
      return isAssistantMessage(message.message) ? [{ type: "assistant.started" }] : [];
    case "message_update":
      return normalizeAssistantDelta(message);
    case "message_end":
      return normalizeAssistantCompletion(message.message);
    case "tool_execution_start":
      return normalizeToolStart(message);
    case "tool_execution_update":
      return normalizeToolUpdate(message);
    case "tool_execution_end":
      return normalizeToolEnd(message);
    case "extension_ui_request":
      return normalizeExtensionUIRequest(message);
    case "extension_error": {
      const error = typeof message.error === "string" ? message.error.trim() : "";
      return [
        {
          type: "runtime.error",
          message: error || "A Pi extension failed.",
        },
      ];
    }
    default:
      return [];
  }
}

function normalizeExtensionUIRequest(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  if (message.method === "set_editor_text") {
    const decoded = decodeExtensionUISetEditorTextRequestExit(message);
    return Exit.isSuccess(decoded)
      ? [
          {
            type: "editor-text.requested",
            requestId: decoded.value.id,
            text: decoded.value.text,
          },
        ]
      : [];
  }
  if (
    message.method !== "select" &&
    message.method !== "confirm" &&
    message.method !== "input" &&
    message.method !== "editor"
  ) {
    return [];
  }
  const decoded = decodeExtensionUIDialogRequestExit(message);
  if (Exit.isFailure(decoded)) {
    return [
      {
        type: "extension-ui.invalid",
        ...(message.id !== undefined ? { requestId: message.id } : {}),
        message: "Pi emitted an invalid extension UI dialog request.",
      },
    ];
  }
  const request = decoded.value;
  switch (request.method) {
    case "select":
      return [
        {
          type: "extension-ui.requested",
          requestId: request.id,
          method: request.method,
          title: request.title,
          options: request.options,
          ...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
        },
      ];
    case "confirm":
      return [
        {
          type: "extension-ui.requested",
          requestId: request.id,
          method: request.method,
          title: request.title,
          message: request.message,
          ...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
        },
      ];
    case "input":
      return [
        {
          type: "extension-ui.requested",
          requestId: request.id,
          method: request.method,
          title: request.title,
          ...(request.placeholder !== undefined ? { placeholder: request.placeholder } : {}),
          ...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
        },
      ];
    case "editor":
      return [
        {
          type: "extension-ui.requested",
          requestId: request.id,
          method: request.method,
          title: request.title,
          ...(request.prefill !== undefined ? { prefill: request.prefill } : {}),
        },
      ];
  }
}

function isAssistantMessage(value: unknown): boolean {
  const decoded = decodeMessageExit(value);
  return Exit.isSuccess(decoded) && decoded.value.role === "assistant";
}

function normalizeAssistantDelta(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  const event = message.assistantMessageEvent;
  if (!event || !event.delta || event.delta.length === 0) {
    return [];
  }
  if (event.type !== "text_delta" && event.type !== "thinking_delta") {
    return [];
  }
  return [
    {
      type: "assistant.delta",
      stream: event.type === "text_delta" ? "text" : "thinking",
      contentIndex: event.contentIndex ?? 0,
      delta: event.delta,
    },
  ];
}

function normalizeAssistantCompletion(value: unknown): ReadonlyArray<PiRpcEvent> {
  const decoded = decodeMessageExit(value);
  if (Exit.isFailure(decoded) || decoded.value.role !== "assistant") {
    return [];
  }
  const stopReason = decoded.value.stopReason?.trim();
  const errorMessage = decoded.value.errorMessage?.trim();
  return [
    {
      type: "assistant.completed",
      ...(stopReason ? { stopReason } : {}),
      ...(errorMessage ? { errorMessage } : {}),
    },
  ];
}

function normalizeToolStart(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  if (!message.toolCallId || !message.toolName) return [];
  return [
    {
      type: "tool.started",
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      itemType: piToolItemType(message.toolName),
      ...(message.args !== undefined ? { args: message.args } : {}),
    },
  ];
}

function normalizeToolUpdate(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  if (!message.toolCallId || !message.toolName) return [];
  const detail = toolResultText(message.partialResult);
  return [
    {
      type: "tool.updated",
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      itemType: piToolItemType(message.toolName),
      ...(detail ? { detail } : {}),
      ...(message.partialResult !== undefined ? { data: message.partialResult } : {}),
    },
  ];
}

function normalizeToolEnd(message: PiWireMessage): ReadonlyArray<PiRpcEvent> {
  if (!message.toolCallId || !message.toolName) return [];
  const detail = toolResultText(message.result);
  return [
    {
      type: "tool.completed",
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      itemType: piToolItemType(message.toolName),
      failed: message.isError === true,
      ...(detail ? { detail } : {}),
      ...(message.result !== undefined ? { data: message.result } : {}),
    },
  ];
}

function piToolItemType(toolName: string): ToolLifecycleItemType {
  switch (toolName.trim().toLowerCase()) {
    case "bash":
    case "command":
    case "exec":
    case "shell":
      return "command_execution";
    case "edit":
    case "write":
    case "apply_patch":
    case "create_file":
      return "file_change";
    case "web_search":
    case "search":
      return "web_search";
    case "view_image":
      return "image_view";
    default:
      return "dynamic_tool_call";
  }
}

function toolResultText(value: unknown): string | undefined {
  const decoded = decodeToolResultExit(value);
  if (Exit.isFailure(decoded)) return undefined;
  const text = decoded.value.content
    ?.filter((part) => part.type === "text" && part.text !== undefined)
    .map((part) => part.text)
    .join("\n")
    .trim();
  return text && text.length > 0 ? text : undefined;
}

import {
  EventId,
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import type { PiRpcEvent } from "../pi/PiRpcProtocol.ts";
import {
  makePiRpcConnection,
  type PiRpcConnection,
  type PiThinkingLevel,
} from "../pi/PiRpcTransport.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_RESUME_VERSION = 1 as const;

const PiResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(PI_RESUME_VERSION),
  sessionFile: Schema.String,
  sessionId: Schema.optional(Schema.String),
});
const decodeResumeCursor = Schema.decodeUnknownExit(PiResumeCursor);

const PiState = Schema.Struct({
  sessionFile: Schema.optional(Schema.String),
  sessionId: Schema.optional(Schema.String),
  model: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        provider: Schema.String,
        id: Schema.String,
      }),
    ),
  ),
});
const decodePiState = Schema.decodeUnknownExit(PiState);

const PiCommands = Schema.Struct({
  commands: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      description: Schema.optional(Schema.String),
    }),
  ),
});
const decodePiCommands = Schema.decodeUnknownExit(PiCommands);

const PiForkMessages = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({
      entryId: Schema.String,
      text: Schema.String,
    }),
  ),
});
const decodePiForkMessages = Schema.decodeUnknownExit(PiForkMessages);

const PiForkResult = Schema.Struct({
  text: Schema.String,
  cancelled: Schema.Boolean,
});
const decodePiForkResult = Schema.decodeUnknownExit(PiForkResult);

interface PiTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

type PiExtensionUIRequest = Extract<PiRpcEvent, { readonly type: "extension-ui.requested" }>;

interface PiPendingInteraction {
  readonly request: PiExtensionUIRequest;
  readonly turnId?: TurnId;
}

type PiTurnNotificationLevel = "info" | "warning" | "error";

interface PiTurnNotification {
  readonly message: string;
  readonly level: PiTurnNotificationLevel;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly rpc: PiRpcConnection;
  readonly turns: Array<PiTurnSnapshot>;
  readonly pendingInteractions: Map<string, PiPendingInteraction>;
  readonly interactionMutex: Semaphore.Semaphore;
  activeTurnId: TurnId | undefined;
  assistantItemId: RuntimeItemId | undefined;
  assistantStopReason: string | undefined;
  lastTurnNotification: PiTurnNotification | undefined;
  promptReturned: boolean;
  runStarted: boolean;
  interrupted: boolean;
  stopped: boolean;
}

export interface PiAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
}

function resumeCursorFromPiState(state: typeof PiState.Type) {
  return state.sessionFile
    ? {
        schemaVersion: PI_RESUME_VERSION,
        sessionFile: state.sessionFile,
        ...(state.sessionId ? { sessionId: state.sessionId } : {}),
      }
    : undefined;
}

function parseModelSlug(
  slug: string,
): { readonly provider: string; readonly modelId: string } | undefined {
  const separator = slug.indexOf("/");
  if (separator < 1 || separator === slug.length - 1) return undefined;
  return { provider: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

function selectedThinkingLevel(
  options: ReadonlyArray<{ readonly id: string; readonly value: unknown }> | null | undefined,
): PiThinkingLevel | undefined {
  const value = options?.find(
    (option) => option.id === "reasoningEffort" || option.id === "thinkingLevel",
  )?.value;
  return typeof value === "string" &&
    ["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(value)
    ? (value as PiThinkingLevel)
    : undefined;
}

function piSelectOption(rawValue: string, index: number) {
  const label =
    rawValue.length === 0
      ? `(empty value ${index + 1})`
      : rawValue.trim() === rawValue && rawValue.length > 0
        ? rawValue
        : JSON.stringify(rawValue);
  return {
    label,
    description: label === rawValue ? rawValue : `Exact value: ${JSON.stringify(rawValue)}`,
    value: rawValue,
  };
}

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  settings: PiSettings,
  options?: PiAdapterOptions,
): Effect.fn.Return<
  PiAdapterShape,
  never,
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | Scope.Scope
> {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
  const crypto = yield* Crypto.Crypto;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sessions = new Map<ThreadId, PiSessionContext>();
  const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextId = crypto.randomUUIDv4.pipe(Effect.orDie);
  const makeStamp = () =>
    Effect.all({ eventId: Effect.map(nextId, EventId.make), createdAt: nowIso });
  const publish = (event: ProviderRuntimeEvent) =>
    PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);
  const mapRpcError =
    (method: string) => (cause: import("../pi/PiRpcTransport.ts").PiRpcTransportError) =>
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: cause.detail,
        cause,
      });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<PiSessionContext, ProviderAdapterSessionNotFoundError> => {
    const context = sessions.get(threadId);
    return context && !context.stopped && context.session.status !== "error"
      ? Effect.succeed(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const eventBase = (context: PiSessionContext) => ({
    provider: PROVIDER,
    providerInstanceId: boundInstanceId,
    threadId: context.session.threadId,
  });

  const interactionRequestId = (requestId: string) => RuntimeRequestId.make(requestId);

  const publishInteractionResolved = Effect.fn("PiAdapter.publishInteractionResolved")(function* (
    context: PiSessionContext,
    pending: PiPendingInteraction,
    resolution:
      | { readonly kind: "approval"; readonly decision: string }
      | {
          readonly kind: "user-input";
          readonly answers: Record<string, unknown>;
          readonly cancelled?: boolean;
        },
  ) {
    if (resolution.kind === "approval") {
      yield* publish({
        type: "request.resolved",
        ...(yield* makeStamp()),
        ...eventBase(context),
        ...(pending.turnId ? { turnId: pending.turnId } : {}),
        requestId: interactionRequestId(pending.request.requestId),
        payload: {
          requestType: "command_execution_approval",
          decision: resolution.decision,
        },
      });
      return;
    }

    yield* publish({
      type: "user-input.resolved",
      ...(yield* makeStamp()),
      ...eventBase(context),
      ...(pending.turnId ? { turnId: pending.turnId } : {}),
      requestId: interactionRequestId(pending.request.requestId),
      payload: {
        answers: resolution.answers,
        ...(resolution.cancelled !== undefined ? { cancelled: resolution.cancelled } : {}),
      },
    });
  });

  const resolvePendingLocally = Effect.fn("PiAdapter.resolvePendingLocally")(function* (
    context: PiSessionContext,
    requestId: string,
    reason: "cancel" | "timeout",
  ) {
    yield* context.interactionMutex.withPermits(1)(
      Effect.gen(function* () {
        const pending = context.pendingInteractions.get(requestId);
        if (!pending) return;
        context.pendingInteractions.delete(requestId);
        yield* publishInteractionResolved(
          context,
          pending,
          pending.request.method === "confirm"
            ? { kind: "approval", decision: reason === "timeout" ? "decline" : "cancel" }
            : { kind: "user-input", answers: {}, cancelled: true },
        );
      }),
    );
  });

  const clearPendingInteractions = Effect.fn("PiAdapter.clearPendingInteractions")(function* (
    context: PiSessionContext,
    reason: "interrupt" | "settled" | "stop" | "exit" | "prompt-failed",
  ) {
    yield* context.interactionMutex.withPermits(1)(
      Effect.gen(function* () {
        const pendingInteractions = [...context.pendingInteractions.values()];
        context.pendingInteractions.clear();
        for (const pending of pendingInteractions) {
          if (reason === "interrupt" || reason === "stop" || reason === "prompt-failed") {
            yield* context.rpc
              .respondToExtensionUI({
                type: "extension_ui_response",
                id: pending.request.requestId,
                cancelled: true,
              })
              .pipe(
                Effect.catch((cause) =>
                  Effect.logWarning("Failed to cancel Pi extension UI request", {
                    requestId: pending.request.requestId,
                    detail: cause.detail,
                  }),
                ),
              );
          }
          yield* publishInteractionResolved(
            context,
            pending,
            pending.request.method === "confirm"
              ? {
                  kind: "approval",
                  decision:
                    reason === "settled" &&
                    "timeout" in pending.request &&
                    pending.request.timeout !== undefined &&
                    pending.request.timeout > 0
                      ? "decline"
                      : "cancel",
                }
              : { kind: "user-input", answers: {}, cancelled: true },
          );
        }
      }),
    );
  });

  const publishInteractionOpened = Effect.fn("PiAdapter.publishInteractionOpened")(function* (
    context: PiSessionContext,
    request: PiExtensionUIRequest,
  ) {
    const pending: PiPendingInteraction = {
      request,
      ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
    };
    const duplicate = context.pendingInteractions.has(request.requestId);
    if (duplicate) {
      yield* publish({
        type: "runtime.error",
        ...(yield* makeStamp()),
        ...eventBase(context),
        ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
        payload: {
          message: `Pi reused pending extension UI request id '${request.requestId}'.`,
          class: "validation_error",
        },
      });
      return;
    }
    context.pendingInteractions.set(request.requestId, pending);

    if (request.method === "confirm") {
      const detail = [request.title, request.message]
        .map((part) => part.trim())
        .filter((part) => part.length > 0)
        .join("\n");
      yield* publish({
        type: "request.opened",
        ...(yield* makeStamp()),
        ...eventBase(context),
        ...(pending.turnId ? { turnId: pending.turnId } : {}),
        requestId: interactionRequestId(request.requestId),
        payload: {
          requestType: "command_execution_approval",
          supportsAcceptForSession: false,
          ...(detail ? { detail } : {}),
          args: { method: request.method, title: request.title, message: request.message },
        },
      });
    } else {
      const header =
        request.method === "select"
          ? "Selection"
          : request.method === "editor"
            ? "Editor"
            : "Input";
      const title = request.title.trim() || `Pi ${header.toLowerCase()} request`;
      const options = request.method === "select" ? request.options.map(piSelectOption) : [];
      yield* publish({
        type: "user-input.requested",
        ...(yield* makeStamp()),
        ...eventBase(context),
        ...(pending.turnId ? { turnId: pending.turnId } : {}),
        requestId: interactionRequestId(request.requestId),
        payload: {
          questions: [
            {
              id: "value",
              header,
              question: title,
              options,
              ...(request.method === "input" && request.placeholder !== undefined
                ? { placeholder: request.placeholder }
                : {}),
              ...(request.method === "editor" && request.prefill !== undefined
                ? { defaultValue: request.prefill }
                : {}),
              inputKind: request.method === "editor" ? "multiline" : "text",
              allowEmpty: request.method === "input" || request.method === "editor",
              multiSelect: false,
            },
          ],
          supportsCancellation: true,
        },
      });
    }

    if ("timeout" in request && request.timeout !== undefined && request.timeout > 0) {
      yield* Effect.sleep(Duration.millis(Math.max(0, request.timeout))).pipe(
        Effect.andThen(resolvePendingLocally(context, request.requestId, "timeout")),
        Effect.forkIn(context.scope),
      );
    }
  });

  const settleHandledTurn = Effect.fn("PiAdapter.settleHandledTurn")(function* (
    context: PiSessionContext,
    turnId: TurnId,
  ) {
    const notification = context.lastTurnNotification;
    if (
      context.activeTurnId !== turnId ||
      context.runStarted ||
      !context.promptReturned ||
      !notification
    )
      return;
    yield* clearPendingInteractions(context, "settled");
    const interrupted = context.interrupted;
    context.activeTurnId = undefined;
    context.assistantItemId = undefined;
    context.assistantStopReason = undefined;
    context.lastTurnNotification = undefined;
    context.promptReturned = false;
    context.runStarted = false;
    context.interrupted = false;
    const { lastError: _lastError, ...sessionWithoutLastError } = context.session;
    context.session = {
      ...sessionWithoutLastError,
      status: "ready",
      activeTurnId: undefined,
      updatedAt: yield* nowIso,
    };
    if (!interrupted) {
      yield* publish({
        type: "turn.completed",
        ...(yield* makeStamp()),
        ...eventBase(context),
        turnId,
        payload: {
          state: notification.level === "info" ? "completed" : "failed",
          stopReason:
            notification.level === "info"
              ? null
              : notification.level === "warning"
                ? "blocked"
                : "error",
        },
      });
    }
    yield* publish({
      type: "session.state.changed",
      ...(yield* makeStamp()),
      ...eventBase(context),
      payload: {
        state: "ready",
        reason: interrupted
          ? "Interrupted before Pi started a run"
          : "Pi handled the prompt without starting a run",
      },
    });
  });

  const resetFailedPrompt = Effect.fn("PiAdapter.resetFailedPrompt")(function* (
    context: PiSessionContext,
    turnId: TurnId,
    detail: string,
  ) {
    if (context.activeTurnId !== turnId) return;
    yield* clearPendingInteractions(context, "prompt-failed");
    context.activeTurnId = undefined;
    context.assistantItemId = undefined;
    context.assistantStopReason = undefined;
    context.lastTurnNotification = undefined;
    context.promptReturned = false;
    context.runStarted = false;
    context.interrupted = false;
    context.session = {
      ...context.session,
      status: "ready",
      activeTurnId: undefined,
      lastError: detail,
      updatedAt: yield* nowIso,
    };
    yield* publish({
      type: "turn.aborted",
      ...(yield* makeStamp()),
      ...eventBase(context),
      turnId,
      payload: { reason: detail },
    });
    yield* publish({
      type: "session.state.changed",
      ...(yield* makeStamp()),
      ...eventBase(context),
      payload: { state: "ready", reason: "Pi prompt failed" },
    });
  });

  const handlePiEvent = (context: PiSessionContext, event: PiRpcEvent) =>
    Effect.gen(function* () {
      const turnId = context.activeTurnId;
      switch (event.type) {
        case "run.started":
          if (turnId) {
            context.runStarted = true;
            if (context.interrupted) {
              yield* context.rpc.abort().pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "abort",
                      detail: cause.detail,
                      cause,
                    }),
                ),
              );
              return;
            }
          }
          context.session = {
            ...context.session,
            status: "running",
            updatedAt: yield* nowIso,
          };
          yield* publish({
            type: "session.state.changed",
            ...(yield* makeStamp()),
            ...eventBase(context),
            payload: { state: "running", reason: "Pi is processing the turn" },
          });
          return;
        case "run.ended":
          return;
        case "run.settled": {
          yield* clearPendingInteractions(context, "settled");
          const interrupted = context.interrupted;
          const stopReason = context.assistantStopReason;
          const failed = stopReason === "error";
          context.activeTurnId = undefined;
          context.assistantItemId = undefined;
          context.assistantStopReason = undefined;
          context.lastTurnNotification = undefined;
          context.promptReturned = false;
          context.runStarted = false;
          context.interrupted = false;
          const { lastError: _lastError, ...sessionWithoutLastError } = context.session;
          context.session = {
            ...sessionWithoutLastError,
            status: "ready",
            activeTurnId: undefined,
            updatedAt: yield* nowIso,
          };
          if (turnId && !interrupted) {
            yield* publish({
              type: "turn.completed",
              ...(yield* makeStamp()),
              ...eventBase(context),
              turnId,
              payload: {
                state: failed ? "failed" : "completed",
                stopReason: stopReason ?? null,
              },
            });
          }
          yield* publish({
            type: "session.state.changed",
            ...(yield* makeStamp()),
            ...eventBase(context),
            payload: { state: "ready", reason: "Pi turn settled" },
          });
          return;
        }
        case "assistant.started": {
          if (!turnId || context.interrupted) return;
          const itemId = RuntimeItemId.make(`${turnId}:assistant`);
          context.assistantItemId = itemId;
          yield* publish({
            type: "item.started",
            ...(yield* makeStamp()),
            ...eventBase(context),
            turnId,
            itemId,
            payload: { itemType: "assistant_message", status: "inProgress" },
          });
          return;
        }
        case "assistant.delta":
          if (context.interrupted || !turnId || !context.assistantItemId) return;
          yield* publish({
            type: "content.delta",
            ...(yield* makeStamp()),
            ...eventBase(context),
            turnId,
            itemId: context.assistantItemId,
            payload: {
              streamKind: event.stream === "text" ? "assistant_text" : "reasoning_text",
              delta: event.delta,
              contentIndex: event.contentIndex,
            },
          });
          return;
        case "assistant.completed":
          if (context.interrupted) return;
          context.assistantStopReason = event.stopReason;
          if (!turnId || !context.assistantItemId) return;
          yield* publish({
            type: "item.completed",
            ...(yield* makeStamp()),
            ...eventBase(context),
            turnId,
            itemId: context.assistantItemId,
            payload: {
              itemType: "assistant_message",
              status: event.errorMessage ? "failed" : "completed",
              ...(event.errorMessage ? { detail: event.errorMessage } : {}),
            },
          });
          return;
        case "tool.started": {
          if (!turnId || context.interrupted) return;
          const itemId = RuntimeItemId.make(event.toolCallId);
          yield* publish({
            type: "item.started",
            ...(yield* makeStamp()),
            ...eventBase(context),
            turnId,
            itemId,
            providerRefs: { providerItemId: ProviderItemId.make(event.toolCallId) },
            payload: {
              itemType: event.itemType,
              status: "inProgress",
              title: event.toolName,
              ...(event.args !== undefined ? { data: event.args } : {}),
            },
          });
          return;
        }
        case "tool.updated":
        case "tool.completed": {
          if (!turnId || context.interrupted) return;
          const completed = event.type === "tool.completed";
          yield* publish({
            type: completed ? "item.completed" : "item.updated",
            ...(yield* makeStamp()),
            ...eventBase(context),
            turnId,
            itemId: RuntimeItemId.make(event.toolCallId),
            providerRefs: { providerItemId: ProviderItemId.make(event.toolCallId) },
            payload: {
              itemType: event.itemType,
              status: completed ? (event.failed ? "failed" : "completed") : "inProgress",
              title: event.toolName,
              ...(event.detail ? { detail: event.detail } : {}),
              ...(event.data !== undefined ? { data: event.data } : {}),
            },
          } as ProviderRuntimeEvent);
          return;
        }
        case "extension-ui.requested":
          yield* publishInteractionOpened(context, event);
          return;
        case "editor-text.requested":
          if (context.interrupted) return;
          yield* publish({
            type: "composer.text.requested",
            ...(yield* makeStamp()),
            ...eventBase(context),
            requestId: RuntimeRequestId.make(event.requestId),
            payload: { text: event.text },
          });
          return;
        case "extension-ui.notified":
          if (turnId && event.observedDuringPromptRequest === true) {
            context.lastTurnNotification = { message: event.message, level: event.level };
          }
          if (event.level === "error") {
            yield* publish({
              type: "runtime.error",
              ...(yield* makeStamp()),
              ...eventBase(context),
              ...(turnId ? { turnId } : {}),
              payload: { message: event.message, class: "provider_error" },
            });
          } else if (event.level === "warning") {
            yield* publish({
              type: "runtime.warning",
              ...(yield* makeStamp()),
              ...eventBase(context),
              ...(turnId ? { turnId } : {}),
              payload: { message: event.message },
            });
          } else {
            yield* publish({
              type: "runtime.info",
              ...(yield* makeStamp()),
              ...eventBase(context),
              ...(turnId ? { turnId } : {}),
              payload: { message: event.message },
            });
          }
          if (turnId && event.observedDuringPromptRequest === true) {
            yield* settleHandledTurn(context, turnId);
          }
          return;
        case "extension-ui.invalid":
          if (event.requestId !== undefined) {
            yield* context.rpc
              .respondToExtensionUI({
                type: "extension_ui_response",
                id: event.requestId,
                cancelled: true,
              })
              .pipe(
                Effect.catch((cause) =>
                  Effect.logWarning("Failed to cancel invalid Pi extension UI request", {
                    requestId: event.requestId,
                    detail: cause.detail,
                  }),
                ),
              );
          }
          yield* publish({
            type: "runtime.error",
            ...(yield* makeStamp()),
            ...eventBase(context),
            ...(turnId ? { turnId } : {}),
            payload: { message: event.message, class: "validation_error" },
          });
          return;
        case "runtime.error":
          yield* publish({
            type: "runtime.error",
            ...(yield* makeStamp()),
            ...eventBase(context),
            ...(turnId ? { turnId } : {}),
            payload: { message: event.message, class: "transport_error" },
          });
          return;
        case "runtime.exited":
          yield* clearPendingInteractions(context, "exit");
          context.session = {
            ...context.session,
            status: "error",
            lastError: event.message,
            updatedAt: yield* nowIso,
          };
          yield* publish({
            type: "runtime.error",
            ...(yield* makeStamp()),
            ...eventBase(context),
            ...(turnId ? { turnId } : {}),
            payload: { message: event.message, class: "transport_error" },
          });
          yield* publish({
            type: "session.exited",
            ...(yield* makeStamp()),
            ...eventBase(context),
            payload: { exitKind: "error", reason: event.message, recoverable: true },
          });
      }
    });

  const stopContext = (context: PiSessionContext) =>
    Effect.gen(function* () {
      if (context.stopped) return;
      context.stopped = true;
      yield* clearPendingInteractions(context, "stop");
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
      sessions.delete(context.session.threadId);
      yield* publish({
        type: "session.exited",
        ...(yield* makeStamp()),
        ...eventBase(context),
        payload: { exitKind: "graceful" },
      });
    });

  const startSession: PiAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }
      if (!input.cwd?.trim()) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "cwd is required and must be non-empty.",
        });
      }
      if (!settings.jarvisProjectPath.trim()) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "Configure the Jarvis project path for this provider instance.",
        });
      }

      const existing = sessions.get(input.threadId);
      if (existing && !existing.stopped) yield* stopContext(existing);

      const jarvisRoot = path.resolve(settings.jarvisProjectPath);
      const piCliPath = path.join(
        jarvisRoot,
        "node_modules",
        "@earendil-works",
        "pi-coding-agent",
        "dist",
        "cli.js",
      );
      const extensionPath = path.join(jarvisRoot, "src", "extension.ts");
      const missing = [] as Array<string>;
      const exists = (candidate: string) =>
        fileSystem.exists(candidate).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: `Failed to inspect Jarvis runtime path: ${candidate}`,
                cause,
              }),
          ),
        );
      if (!(yield* exists(piCliPath))) missing.push(piCliPath);
      if (!(yield* exists(extensionPath))) missing.push(extensionPath);
      if (missing.length > 0) {
        return yield* new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId: input.threadId,
          detail: `Jarvis runtime file not found: ${missing.join(", ")}`,
        });
      }

      const resume = decodeResumeCursor(input.resumeCursor);
      const sessionScope = yield* Scope.make("sequential");
      let transferred = false;
      yield* Effect.addFinalizer(() =>
        transferred ? Effect.void : Scope.close(sessionScope, Exit.void),
      );
      const rpc = yield* makePiRpcConnection({
        nodePath: process.execPath,
        piCliPath,
        extensionPath,
        cwd: path.resolve(input.cwd.trim()),
        ...(Exit.isSuccess(resume) ? { sessionPath: resume.value.sessionFile } : {}),
        additionalArgs: tokenizeCliArgs(settings.launchArgs),
        environment: { ...options?.environment, PI_OAUTH_CALLBACK_HOST: "::" },
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        Effect.provideService(Scope.Scope, sessionScope),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: cause.detail,
              cause,
            }),
        ),
      );

      const commandsResponse = yield* rpc
        .request({ type: "get_commands" })
        .pipe(Effect.mapError(mapRpcError("get_commands")));
      const commands = decodePiCommands(commandsResponse.data);
      if (
        Exit.isFailure(commands) ||
        !commands.value.commands.some((command) => command.name === "jarvis")
      ) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "Pi started, but the Jarvis extension did not register /jarvis.",
        });
      }

      const selection =
        input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
      if (selection) {
        const model = parseModelSlug(selection.model);
        if (!model) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Pi model slugs must use '<provider>/<model-id>'.",
          });
        }
        yield* rpc
          .request({ type: "set_model", ...model })
          .pipe(Effect.mapError(mapRpcError("set_model")));
        const thinking = selectedThinkingLevel(selection.options);
        if (thinking) {
          yield* rpc
            .request({ type: "set_thinking_level", level: thinking })
            .pipe(Effect.mapError(mapRpcError("set_thinking_level")));
        }
      }
      if (input.title) {
        yield* rpc
          .request({ type: "set_session_name", name: input.title })
          .pipe(Effect.mapError(mapRpcError("set_session_name")));
      }

      const stateResponse = yield* rpc.getState().pipe(Effect.mapError(mapRpcError("get_state")));
      const state = decodePiState(stateResponse);
      const now = yield* nowIso;
      const resumeCursor = Exit.isSuccess(state) ? resumeCursorFromPiState(state.value) : undefined;
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd: path.resolve(input.cwd.trim()),
        ...(selection ? { model: selection.model } : {}),
        threadId: input.threadId,
        ...(resumeCursor ? { resumeCursor } : {}),
        createdAt: now,
        updatedAt: now,
      };
      const context: PiSessionContext = {
        session,
        scope: sessionScope,
        rpc,
        turns: [],
        pendingInteractions: new Map(),
        interactionMutex: yield* Semaphore.make(1),
        activeTurnId: undefined,
        assistantItemId: undefined,
        assistantStopReason: undefined,
        lastTurnNotification: undefined,
        promptReturned: false,
        runStarted: false,
        interrupted: false,
        stopped: false,
      };
      sessions.set(input.threadId, context);
      transferred = true;
      yield* rpc.events.pipe(
        Stream.runForEach((event) => handlePiEvent(context, event)),
        Effect.catchCause((cause) => Effect.logError("Pi RPC event pump failed", { cause })),
        Effect.forkIn(sessionScope),
      );

      yield* publish({
        type: "session.started",
        ...(yield* makeStamp()),
        ...eventBase(context),
        payload: {
          message: "Jarvis Pi RPC session ready",
          ...(resumeCursor ? { resume: resumeCursor } : {}),
        },
      });
      yield* publish({
        type: "thread.started",
        ...(yield* makeStamp()),
        ...eventBase(context),
        payload:
          Exit.isSuccess(state) && state.value.sessionId
            ? { providerThreadId: state.value.sessionId }
            : {},
      });
      return session;
    }).pipe(Effect.scoped);

  const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      if (context.activeTurnId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Pi is already processing a turn. Interrupt it before starting another.",
        });
      }
      if (input.attachments && input.attachments.length > 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Attachments are not supported by the Day 1 Pi adapter.",
        });
      }
      const message = input.input?.trim();
      if (!message) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Turn requires non-empty text.",
        });
      }
      const selection =
        input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
      if (selection && selection.model !== context.session.model) {
        const model = parseModelSlug(selection.model);
        if (!model) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Pi model slugs must use '<provider>/<model-id>'.",
          });
        }
        yield* context.rpc
          .request({ type: "set_model", ...model })
          .pipe(Effect.mapError(mapRpcError("set_model")));
        const thinking = selectedThinkingLevel(selection.options);
        if (thinking) {
          yield* context.rpc
            .request({ type: "set_thinking_level", level: thinking })
            .pipe(Effect.mapError(mapRpcError("set_thinking_level")));
        }
      }
      const turnId = TurnId.make(yield* nextId);
      context.activeTurnId = turnId;
      context.lastTurnNotification = undefined;
      context.promptReturned = false;
      context.runStarted = false;
      context.interrupted = false;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        ...(selection ? { model: selection.model } : {}),
        updatedAt: yield* nowIso,
      };
      context.turns.push({ id: turnId, items: [{ role: "user", content: message }] });
      yield* publish({
        type: "turn.started",
        ...(yield* makeStamp()),
        ...eventBase(context),
        turnId,
        payload: context.session.model ? { model: context.session.model } : {},
      });
      yield* context.rpc.prompt(message).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: cause.detail,
              cause,
            }),
        ),
        Effect.tapError((requestError) => resetFailedPrompt(context, turnId, requestError.detail)),
      );
      if (context.activeTurnId === turnId) {
        context.promptReturned = true;
        yield* settleHandledTurn(context, turnId);
      }
      return {
        threadId: input.threadId,
        turnId,
        ...(context.session.resumeCursor !== undefined
          ? { resumeCursor: context.session.resumeCursor }
          : {}),
      };
    });

  const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const turnId = context.activeTurnId;
      context.interrupted = true;
      yield* clearPendingInteractions(context, "interrupt");
      yield* context.rpc.abort().pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "abort",
              detail: cause.detail,
              cause,
            }),
        ),
      );
      if (turnId) {
        yield* publish({
          type: "turn.aborted",
          ...(yield* makeStamp()),
          ...eventBase(context),
          turnId,
          payload: { reason: "Interrupted by user" },
        });
      }
    });

  const readThread: PiAdapterShape["readThread"] = (threadId) =>
    Effect.map(requireSession(threadId), (context) => ({ threadId, turns: context.turns }));
  const rollbackThread: PiAdapterShape["rollbackThread"] = (threadId, numTurns) =>
    Effect.gen(function* () {
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }

      const context = yield* requireSession(threadId);
      if (context.activeTurnId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Interrupt the active Pi turn before rolling back the thread.",
        });
      }

      const messagesResponse = yield* context.rpc
        .request({ type: "get_fork_messages" })
        .pipe(Effect.mapError(mapRpcError("get_fork_messages")));
      const forkMessages = decodePiForkMessages(messagesResponse.data);
      if (Exit.isFailure(forkMessages)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "get_fork_messages",
          detail: "Pi returned invalid fork-message data.",
          cause: forkMessages.cause,
        });
      }

      if (forkMessages.value.messages.length === 0) {
        return {
          threadId,
          turns: context.turns,
          ...(context.session.resumeCursor !== undefined
            ? { resumeCursor: context.session.resumeCursor }
            : {}),
        };
      }

      const targetIndex = Math.max(0, forkMessages.value.messages.length - numTurns);
      const target = forkMessages.value.messages[targetIndex];
      if (!target) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "get_fork_messages",
          detail: "Pi did not return a rollback target.",
        });
      }

      const forkResponse = yield* context.rpc
        .request({ type: "fork", entryId: target.entryId })
        .pipe(Effect.mapError(mapRpcError("fork")));
      const forkResult = decodePiForkResult(forkResponse.data);
      if (Exit.isFailure(forkResult)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "fork",
          detail: "Pi returned invalid fork data.",
          cause: forkResult.cause,
        });
      }
      if (forkResult.value.cancelled) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "fork",
          detail: "A Pi extension cancelled the rollback.",
        });
      }

      const stateResponse = yield* context.rpc
        .getState()
        .pipe(Effect.mapError(mapRpcError("get_state")));
      const state = decodePiState(stateResponse);
      const resumeCursor = Exit.isSuccess(state) ? resumeCursorFromPiState(state.value) : undefined;
      if (!resumeCursor) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "get_state",
          detail: "Pi did not report resume state after rollback.",
          ...(Exit.isFailure(state) ? { cause: state.cause } : {}),
        });
      }

      context.turns.splice(Math.max(0, context.turns.length - numTurns));
      context.session = {
        ...context.session,
        resumeCursor,
        updatedAt: yield* nowIso,
      };
      return { threadId, turns: context.turns, resumeCursor };
    });
  const respondToRequest: PiAdapterShape["respondToRequest"] = (threadId, requestId, decision) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* context.interactionMutex.withPermits(1)(
        Effect.gen(function* () {
          const pending = context.pendingInteractions.get(requestId);
          if (!pending || pending.request.method !== "confirm") {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "extension_ui_response",
              detail: `Unknown pending approval request: ${requestId}`,
            });
          }
          if (decision === "acceptForSession") {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "extension_ui_response",
              detail: "Pi confirmation requests do not support session-persistent approval.",
            });
          }
          const response =
            decision === "cancel"
              ? ({
                  type: "extension_ui_response",
                  id: pending.request.requestId,
                  cancelled: true,
                } as const)
              : ({
                  type: "extension_ui_response",
                  id: pending.request.requestId,
                  confirmed: decision === "accept",
                } as const);
          yield* context.rpc
            .respondToExtensionUI(response)
            .pipe(Effect.mapError(mapRpcError("extension_ui_response")));
          context.pendingInteractions.delete(requestId);
          yield* publishInteractionResolved(context, pending, {
            kind: "approval",
            decision,
          });
        }),
      );
    });
  const respondToUserInput: PiAdapterShape["respondToUserInput"] = (
    threadId,
    requestId,
    answers,
    cancelled,
  ) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* context.interactionMutex.withPermits(1)(
        Effect.gen(function* () {
          const pending = context.pendingInteractions.get(requestId);
          if (!pending || pending.request.method === "confirm") {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "extension_ui_response",
              detail: `Unknown pending user-input request: ${requestId}`,
            });
          }
          if (cancelled === true) {
            yield* context.rpc
              .respondToExtensionUI({
                type: "extension_ui_response",
                id: pending.request.requestId,
                cancelled: true,
              })
              .pipe(Effect.mapError(mapRpcError("extension_ui_response")));
            context.pendingInteractions.delete(requestId);
            yield* publishInteractionResolved(context, pending, {
              kind: "user-input",
              answers: {},
              cancelled: true,
            });
            return;
          }
          const value = answers.value;
          if (typeof value !== "string") {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "extension_ui_response",
              detail: "Pi user-input response requires a string answer named 'value'.",
            });
          }
          if (pending.request.method === "select" && !pending.request.options.includes(value)) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "extension_ui_response",
              detail: `Pi selection response must match one of the offered options: ${pending.request.options.join(", ")}`,
            });
          }
          yield* context.rpc
            .respondToExtensionUI({
              type: "extension_ui_response",
              id: pending.request.requestId,
              value,
            })
            .pipe(Effect.mapError(mapRpcError("extension_ui_response")));
          context.pendingInteractions.delete(requestId);
          yield* publishInteractionResolved(context, pending, {
            kind: "user-input",
            answers,
          });
        }),
      );
    });
  const stopSession: PiAdapterShape["stopSession"] = (threadId) => {
    const context = sessions.get(threadId);
    return context
      ? stopContext(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };
  const listSessions: PiAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), (context) => ({ ...context.session })));
  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => Option.fromUndefinedOr(sessions.get(threadId))).pipe(
      Effect.map(
        Option.exists((context) => !context.stopped && context.session.status !== "error"),
      ),
    );
  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(sessions.values(), stopContext, { discard: true });

  yield* Effect.addFinalizer(() =>
    stopAll().pipe(
      Effect.catchCause((cause) => Effect.logError("Pi adapter shutdown failed", { cause })),
      Effect.tap(() => PubSub.shutdown(runtimeEvents)),
    ),
  );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromPubSub(runtimeEvents),
  } satisfies PiAdapterShape;
});

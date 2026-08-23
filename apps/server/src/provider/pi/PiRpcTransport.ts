import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  decodePiRpcLine,
  normalizePiRpcEvent,
  type PiRpcEvent,
  type PiRpcResponse,
  responseFromPiWireMessage,
} from "./PiRpcProtocol.ts";

const PiRpcTransportOperation = Schema.Literals(["spawn", "write", "response", "process"]);

export class PiRpcTransportError extends Schema.TaggedErrorClass<PiRpcTransportError>()(
  "PiRpcTransportError",
  {
    operation: PiRpcTransportOperation,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type PiRpcCommand =
  | { readonly type: "prompt"; readonly message: string }
  | { readonly type: "abort" }
  | { readonly type: "get_state" }
  | { readonly type: "get_available_models" }
  | { readonly type: "get_commands" }
  | { readonly type: "get_messages" }
  | { readonly type: "get_entries"; readonly since?: string }
  | { readonly type: "get_fork_messages" }
  | { readonly type: "fork"; readonly entryId: string }
  | { readonly type: "set_model"; readonly provider: string; readonly modelId: string }
  | { readonly type: "set_thinking_level"; readonly level: PiThinkingLevel }
  | { readonly type: "set_session_name"; readonly name: string }
  | { readonly type: "new_session"; readonly parentSession?: string }
  | { readonly type: "switch_session"; readonly sessionPath: string };

export type PiExtensionUIResponse =
  | { readonly type: "extension_ui_response"; readonly id: string; readonly value: string }
  | { readonly type: "extension_ui_response"; readonly id: string; readonly confirmed: boolean }
  | { readonly type: "extension_ui_response"; readonly id: string; readonly cancelled: true };

export type PiThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";

const PiThinkingLevel = Schema.Literals([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);

const PiRpcWireCommand = Schema.Union([
  Schema.Struct({ id: Schema.String, type: Schema.Literal("prompt"), message: Schema.String }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("abort") }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("get_state") }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("get_available_models") }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("get_commands") }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("get_messages") }),
  Schema.Struct({ id: Schema.String, type: Schema.Literal("get_fork_messages") }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("fork"),
    entryId: Schema.String,
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("get_entries"),
    since: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("set_model"),
    provider: Schema.String,
    modelId: Schema.String,
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("set_thinking_level"),
    level: PiThinkingLevel,
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("set_session_name"),
    name: Schema.String,
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("new_session"),
    parentSession: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("switch_session"),
    sessionPath: Schema.String,
  }),
]);

const PiExtensionUIResponse = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    value: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    confirmed: Schema.Boolean,
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    cancelled: Schema.Literal(true),
  }),
]);

const encodePiRpcCommand = Schema.encodeEffect(Schema.fromJsonString(PiRpcWireCommand));
const encodePiExtensionUIResponse = Schema.encodeEffect(
  Schema.fromJsonString(PiExtensionUIResponse),
);

export interface PiRpcLaunchInput {
  readonly nodePath: string;
  readonly piCliPath: string;
  readonly extensionPath: string;
  readonly cwd: string;
  readonly sessionPath?: string;
  readonly additionalArgs?: ReadonlyArray<string>;
  readonly environment?: Record<string, string | undefined>;
}

export interface PiRpcConnection {
  readonly pid: number;
  readonly events: Stream.Stream<PiRpcEvent>;
  readonly request: (command: PiRpcCommand) => Effect.Effect<PiRpcResponse, PiRpcTransportError>;
  readonly respondToExtensionUI: (
    response: PiExtensionUIResponse,
  ) => Effect.Effect<void, PiRpcTransportError>;
  readonly prompt: (message: string) => Effect.Effect<void, PiRpcTransportError>;
  readonly abort: () => Effect.Effect<void, PiRpcTransportError>;
  readonly getState: () => Effect.Effect<unknown, PiRpcTransportError>;
  readonly stderr: Effect.Effect<string>;
}

export interface PiRpcLineSplit {
  readonly remainder: string;
  readonly lines: ReadonlyArray<string>;
}

/** Pi RPC uses LF as its only record delimiter. A CR is framing only when it precedes LF. */
export function splitPiRpcStdoutChunk(remainder: string, chunk: string): PiRpcLineSplit {
  const input = `${remainder}${chunk}`;
  const lines: Array<string> = [];
  let offset = 0;
  for (;;) {
    const lineFeed = input.indexOf("\n", offset);
    if (lineFeed < 0) break;
    const end = lineFeed > offset && input[lineFeed - 1] === "\r" ? lineFeed - 1 : lineFeed;
    lines.push(input.slice(offset, end));
    offset = lineFeed + 1;
  }
  return { remainder: input.slice(offset), lines };
}

export function buildPiRpcLaunchArgs(input: PiRpcLaunchInput): ReadonlyArray<string> {
  return [
    input.piCliPath,
    "--mode",
    "rpc",
    "--no-extensions",
    "--extension",
    input.extensionPath,
    ...(input.sessionPath ? ["--session", input.sessionPath] : []),
    ...(input.additionalArgs ?? []),
  ];
}

export const makePiRpcConnection = Effect.fn("makePiRpcConnection")(function* (
  input: PiRpcLaunchInput,
): Effect.fn.Return<
  PiRpcConnection,
  PiRpcTransportError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const scope = yield* Scope.Scope;
  const events = yield* Queue.unbounded<PiRpcEvent>();
  const pending = yield* Ref.make(
    new Map<string, Deferred.Deferred<PiRpcResponse, PiRpcTransportError>>(),
  );
  const requestSequence = yield* Ref.make(0);
  const active = yield* Ref.make(true);
  const stderr = yield* Ref.make("");
  const writeMutex = yield* Semaphore.make(1);

  yield* Scope.addFinalizer(scope, Queue.shutdown(events));

  const command = ChildProcess.make(input.nodePath, buildPiRpcLaunchArgs(input), {
    cwd: input.cwd,
    ...(input.environment ? { env: input.environment, extendEnv: true } : {}),
    stdin: { stream: "pipe", endOnDone: false },
    stdout: "pipe",
    stderr: "pipe",
    killSignal: "SIGTERM",
    forceKillAfter: "2 seconds",
  });

  const handle = yield* Effect.acquireRelease(
    spawner.spawn(command).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcTransportError({
            operation: "spawn",
            detail: `Failed to start Pi RPC with '${input.nodePath}'.`,
            cause,
          }),
      ),
    ),
    (child) => child.kill().pipe(Effect.ignore),
  );

  const failPending = Effect.fn("PiRpcConnection.failPending")(function* (
    error: PiRpcTransportError,
  ) {
    const requests = yield* Ref.getAndSet(pending, new Map());
    yield* Effect.forEach(requests.values(), (deferred) => Deferred.fail(deferred, error), {
      discard: true,
    });
  });

  const takePending = (id: string) =>
    Ref.modify(pending, (requests) => {
      const next = new Map(requests);
      const deferred = next.get(id);
      next.delete(id);
      return [Option.fromUndefinedOr(deferred), next] as const;
    });

  const processLine = Effect.fn("PiRpcConnection.processLine")(function* (line: string) {
    const trimmed = line.trim();
    if (trimmed.length === 0) return;
    const decoded = yield* Effect.exit(decodePiRpcLine(trimmed));
    if (decoded._tag === "Failure") {
      yield* Queue.offer(events, {
        type: "runtime.error",
        message: "Pi emitted an invalid JSONL message.",
      });
      return;
    }

    const response = responseFromPiWireMessage(decoded.value);
    if (response) {
      const deferred = yield* takePending(
        response._tag === "success" ? response.response.id : response.id,
      );
      if (Option.isNone(deferred)) return;
      if (response._tag === "success") {
        yield* Deferred.succeed(deferred.value, response.response);
      } else {
        yield* Deferred.fail(
          deferred.value,
          new PiRpcTransportError({
            operation: "response",
            detail: response.error,
          }),
        );
      }
      return;
    }
    if (decoded.value.type === "response") {
      const detail = "Pi emitted a malformed command response.";
      if (decoded.value.id) {
        const deferred = yield* takePending(decoded.value.id);
        if (Option.isSome(deferred)) {
          yield* Deferred.fail(
            deferred.value,
            new PiRpcTransportError({ operation: "response", detail }),
          );
        }
      }
      yield* Queue.offer(events, { type: "runtime.error", message: detail });
      return;
    }

    yield* Effect.forEach(
      normalizePiRpcEvent(decoded.value),
      (event) => Queue.offer(events, event),
      {
        discard: true,
      },
    );
  });

  yield* handle.stdout.pipe(
    Stream.decodeText(),
    Stream.mapAccum(
      () => "",
      (remainder, chunk) => {
        const split = splitPiRpcStdoutChunk(remainder, chunk);
        return [split.remainder, split.lines] as const;
      },
    ),
    Stream.runForEach(processLine),
    Effect.catch((cause) =>
      Queue.offer(events, {
        type: "runtime.error",
        message: `Pi RPC stdout failed: ${String(cause)}`,
      }).pipe(Effect.asVoid),
    ),
    Effect.forkIn(scope),
  );

  yield* handle.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Ref.update(stderr, (current) => `${current}${chunk}`.slice(-16_384)),
    ),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  yield* handle.exitCode.pipe(
    Effect.flatMap((exitCode) =>
      Effect.gen(function* () {
        yield* Ref.set(active, false);
        const detail = `Pi RPC exited with code ${Number(exitCode)}.`;
        yield* failPending(
          new PiRpcTransportError({
            operation: "process",
            detail,
          }),
        );
        yield* Queue.offer(events, { type: "runtime.exited", message: detail });
      }),
    ),
    Effect.catch((cause) => {
      const error = new PiRpcTransportError({
        operation: "process",
        detail: "Pi RPC exited before reporting an exit code.",
        cause,
      });
      return failPending(error).pipe(
        Effect.andThen(Queue.offer(events, { type: "runtime.exited", message: error.detail })),
      );
    }),
    Effect.forkIn(scope),
  );

  const request = Effect.fn("PiRpcConnection.request")(function* (
    rpcCommand: PiRpcCommand,
  ): Effect.fn.Return<PiRpcResponse, PiRpcTransportError> {
    if (!(yield* Ref.get(active))) {
      return yield* new PiRpcTransportError({
        operation: "write",
        detail: "Pi RPC is no longer running.",
      });
    }

    const id = `t3-pi-${yield* Ref.updateAndGet(requestSequence, (value) => value + 1)}`;
    const deferred = yield* Deferred.make<PiRpcResponse, PiRpcTransportError>();
    yield* Ref.update(pending, (requests) => {
      const next = new Map(requests);
      next.set(id, deferred);
      return next;
    });

    const encoded = yield* encodePiRpcCommand({ id, ...rpcCommand }).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcTransportError({
            operation: "write",
            detail: `Failed to encode '${rpcCommand.type}' for Pi RPC.`,
            cause,
          }),
      ),
    );
    const payload = `${encoded}\n`;
    yield* writeMutex
      .withPermits(1)(
        Stream.run(Stream.encodeText(Stream.make(payload)), handle.stdin).pipe(
          Effect.mapError(
            (cause) =>
              new PiRpcTransportError({
                operation: "write",
                detail: `Failed to send '${rpcCommand.type}' to Pi RPC.`,
                cause,
              }),
          ),
        ),
      )
      .pipe(Effect.tapError(() => takePending(id)));

    const response = yield* Deferred.await(deferred).pipe(Effect.ensuring(takePending(id)));
    if (response.command !== rpcCommand.type) {
      return yield* new PiRpcTransportError({
        operation: "response",
        detail: `Pi responded to '${rpcCommand.type}' with '${response.command}'.`,
      });
    }
    return response;
  });

  const respondToExtensionUI = Effect.fn("PiRpcConnection.respondToExtensionUI")(function* (
    response: PiExtensionUIResponse,
  ): Effect.fn.Return<void, PiRpcTransportError> {
    if (!(yield* Ref.get(active))) {
      return yield* new PiRpcTransportError({
        operation: "write",
        detail: "Pi RPC is no longer running.",
      });
    }

    const encoded = yield* encodePiExtensionUIResponse(response).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcTransportError({
            operation: "write",
            detail: "Failed to encode a Pi extension UI response.",
            cause,
          }),
      ),
    );
    const payload = `${encoded}\n`;
    yield* writeMutex.withPermits(1)(
      Stream.run(Stream.encodeText(Stream.make(payload)), handle.stdin).pipe(
        Effect.mapError(
          (cause) =>
            new PiRpcTransportError({
              operation: "write",
              detail: "Failed to send a Pi extension UI response.",
              cause,
            }),
        ),
      ),
    );
  });

  return {
    pid: Number(handle.pid),
    events: Stream.fromQueue(events),
    request,
    respondToExtensionUI,
    prompt: (message) => request({ type: "prompt", message }).pipe(Effect.asVoid),
    abort: () => request({ type: "abort" }).pipe(Effect.asVoid),
    getState: () => request({ type: "get_state" }).pipe(Effect.map((response) => response.data)),
    stderr: Ref.get(stderr),
  } satisfies PiRpcConnection;
});

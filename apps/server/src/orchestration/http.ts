import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Context from "effect/Context";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { normalizeDispatchCommand } from "./Normalizer.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import {
  makeDurableOwnerPresentation,
  rememberPiInstances,
} from "../provider/pi/OwnerThreadPresentation.ts";
import { ServerSettingsService } from "../serverSettings.ts";

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const { presentShell, presentThread, presentThreadSnapshot, validateDestination } =
      yield* makeDurableOwnerPresentation;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const serverSettings = Context.getOption(yield* Effect.context<never>(), ServerSettingsService);
    if (Option.isSome(serverSettings))
      rememberPiInstances((yield* serverSettings.value.getSettings).providerInstances);

    return handlers
      .handle(
        "snapshot",
        Effect.fn("environment.orchestration.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          // Serve the lightweight command read model (thread bodies empty)
          // instead of the fully hydrated snapshot. Hydrating every message
          // and activity payload in the database has OOM-killed servers, and
          // the route's only consumer (the project CLI) reads projects alone —
          // UI clients load the shell and per-thread snapshots instead.
          return yield* projectionSnapshotQuery.getCommandReadModel().pipe(
            Effect.flatMap((snapshot) =>
              Effect.promise(async () => ({
                ...snapshot,
                threads: await Promise.all(snapshot.threads.map((thread) => presentThread(thread))),
              })),
            ),
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_snapshot_failed", cause),
            ),
          );
        }),
      )
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery.getShellSnapshot().pipe(
            Effect.flatMap((snapshot) => Effect.promise(() => presentShell(snapshot))),
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_snapshot_failed", cause),
            ),
          );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* projectionSnapshotQuery
            .getThreadDetailSnapshot(
              args.params.threadId,
              args.payload.turnLimit === undefined
                ? undefined
                : {
                    turnLimit: args.payload.turnLimit,
                    ...(args.payload.beforeCursor !== undefined
                      ? { beforeCursor: args.payload.beforeCursor }
                      : {}),
                  },
            )
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return yield* Effect.promise(() => presentThreadSnapshot(snapshot.value));
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const normalizedCommand = yield* normalizeDispatchCommand(args.payload).pipe(
            Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")),
          );
          if (
            normalizedCommand.type === "thread.turn.start" ||
            normalizedCommand.type === "thread.create" ||
            (normalizedCommand.type === "thread.meta.update" && normalizedCommand.modelSelection)
          ) {
            yield* Effect.gen(function* () {
              const existing = yield* projectionSnapshotQuery.getThreadShellById(
                normalizedCommand.threadId,
              );
              const selection =
                normalizedCommand.modelSelection ??
                (normalizedCommand.type === "thread.turn.start"
                  ? normalizedCommand.bootstrap?.createThread?.modelSelection
                  : undefined) ??
                (Option.isSome(existing) ? existing.value.modelSelection : undefined);
              const settings = Option.isSome(serverSettings)
                ? yield* serverSettings.value.getSettings
                : undefined;
              const driver = selection
                ? settings?.providerInstances[selection.instanceId]?.driver
                : undefined;
              yield* validateDestination(normalizedCommand, driver);
            }).pipe(Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")));
          }
          return yield* orchestrationEngine
            .dispatch(normalizedCommand)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_dispatch_failed", cause),
              ),
            );
        }),
      );
  }),
);

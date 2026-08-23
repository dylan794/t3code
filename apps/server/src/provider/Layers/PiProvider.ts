import {
  type ModelCapabilities,
  type PiSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { makePiRpcConnection } from "../pi/PiRpcTransport.ts";
import * as DateTime from "effect/DateTime";

const PI_PRESENTATION = {
  displayName: "Jarvis (Pi)",
  badgeLabel: "Experimental",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

const PiAvailableModels = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      provider: Schema.String,
      id: Schema.String,
      name: Schema.optional(Schema.String),
    }),
  ),
});
const decodeModels = Schema.decodeUnknownExit(PiAvailableModels);

const PiCommands = Schema.Struct({
  commands: Schema.Array(Schema.Struct({ name: Schema.String })),
});
const decodeCommands = Schema.decodeUnknownExit(PiCommands);

const PiPackage = Schema.Struct({ version: Schema.optional(Schema.String) });
const decodePackage = Schema.decodeUnknownExit(Schema.fromJsonString(PiPackage));

const fallbackModels = (settings: PiSettings) =>
  providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES);

export const buildInitialPiProviderSnapshot = Effect.fn("buildInitialPiProviderSnapshot")(
  function* (settings: PiSettings): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels(settings),
      probe: settings.enabled
        ? {
            installed: settings.jarvisProjectPath.trim().length > 0,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: settings.jarvisProjectPath.trim()
              ? "Checking the Jarvis Pi RPC runtime..."
              : "Configure the Jarvis project path.",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Jarvis (Pi) is disabled in T3 Code settings.",
          },
    });
  },
);

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  if (!settings.enabled) return yield* buildInitialPiProviderSnapshot(settings);
  if (!settings.jarvisProjectPath.trim()) return yield* buildInitialPiProviderSnapshot(settings);

  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
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
  const packagePath = path.join(jarvisRoot, "package.json");

  const probe = Effect.gen(function* () {
    if (!(yield* fileSystem.exists(piCliPath)) || !(yield* fileSystem.exists(extensionPath))) {
      return yield* Effect.fail("missing-runtime" as const);
    }
    const packageContents = yield* fileSystem
      .readFileString(packagePath)
      .pipe(Effect.orElseSucceed(() => "{}"));
    const packageJson = decodePackage(packageContents);
    const version = Exit.isSuccess(packageJson) ? (packageJson.value.version ?? null) : null;

    const rpc = yield* makePiRpcConnection({
      nodePath: process.execPath,
      piCliPath,
      extensionPath,
      cwd: jarvisRoot,
      additionalArgs: ["--no-session"],
      environment: { ...environment, PI_OAUTH_CALLBACK_HOST: "::" },
    });
    const [commandsResponse, modelsResponse] = yield* Effect.all(
      [rpc.request({ type: "get_commands" }), rpc.request({ type: "get_available_models" })],
      { concurrency: "unbounded" },
    );
    const commands = decodeCommands(commandsResponse.data);
    const models = decodeModels(modelsResponse.data);
    if (
      Exit.isFailure(commands) ||
      !commands.value.commands.some((command) => command.name === "jarvis")
    ) {
      return yield* Effect.fail("missing-extension" as const);
    }
    if (Exit.isFailure(models)) return yield* Effect.fail("invalid-models" as const);
    return {
      version,
      models: models.value.models.map(
        (model): ServerProviderModel => ({
          slug: `${model.provider}/${model.id}`,
          name: model.name?.trim() || model.id,
          subProvider: model.provider,
          isCustom: false,
          capabilities: EMPTY_CAPABILITIES,
        }),
      ),
    };
  }).pipe(Effect.scoped, Effect.timeout("15 seconds"));

  const result = yield* Effect.exit(probe);
  if (Exit.isSuccess(result)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: providerModelsFromSettings(
        result.value.models,
        settings.customModels,
        EMPTY_CAPABILITIES,
      ),
      probe: {
        installed: true,
        version: result.value.version,
        status: "ready",
        auth: { status: result.value.models.length > 0 ? "authenticated" : "unknown" },
      },
    });
  }

  const failureText = result.cause.toString();
  const missingRuntime = failureText.includes("missing-runtime");
  const missingExtension = failureText.includes("missing-extension");
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: fallbackModels(settings),
    probe: {
      installed: !missingRuntime,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: missingRuntime
        ? "Jarvis Pi runtime files were not found. Run npm install in the configured project."
        : missingExtension
          ? "Pi started, but the configured Jarvis extension did not register /jarvis."
          : "Jarvis Pi RPC health check failed or timed out.",
    },
  });
});

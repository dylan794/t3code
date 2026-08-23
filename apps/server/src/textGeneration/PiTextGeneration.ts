import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { type ModelSelection, type PiSettings, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const PI_TEXT_GENERATION_TIMEOUT_MS = 180_000;
const PI_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

const PiJsonEvent = Schema.Struct({
  type: Schema.String,
  message: Schema.optional(
    Schema.Struct({
      role: Schema.String,
      errorMessage: Schema.optional(Schema.String),
      content: Schema.optional(
        Schema.Array(
          Schema.Struct({
            type: Schema.String,
            text: Schema.optional(Schema.String),
          }),
        ),
      ),
    }),
  ),
});
const decodePiJsonEvent = Schema.decodeUnknownExit(Schema.fromJsonString(PiJsonEvent));

export interface PiTextGenerationOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nodePath?: string;
  readonly timeoutMs?: number;
}

function selectedThinkingLevel(modelSelection: ModelSelection): string | undefined {
  const value = modelSelection.options?.find(
    (option) => option.id === "reasoningEffort" || option.id === "thinkingLevel",
  )?.value;
  return typeof value === "string" && PI_THINKING_LEVELS.has(value) ? value : undefined;
}

function finalAssistantMessage(
  output: string,
): { readonly text: string; readonly error?: string } | undefined {
  let finalMessage: { readonly text: string; readonly error?: string } | undefined;
  for (const line of output.split(/\r?\n/g)) {
    if (line.trim().length === 0) continue;
    const decoded = decodePiJsonEvent(line);
    if (Exit.isFailure(decoded)) return undefined;
    if (decoded.value.type !== "message_end" || decoded.value.message?.role !== "assistant") {
      continue;
    }
    const message = decoded.value.message;
    const text =
      message.content
        ?.filter(
          (part): part is { readonly type: string; readonly text: string } =>
            part.type === "text" && part.text !== undefined,
        )
        .map((part) => part.text)
        .join("") ?? "";
    finalMessage = {
      text,
      ...(message.errorMessage?.trim() ? { error: message.errorMessage.trim() } : {}),
    };
  }
  return finalMessage;
}

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  settings: PiSettings,
  options?: PiTextGenerationOptions,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const environment = {
    ...(options?.environment ?? process.env),
    PI_OAUTH_CALLBACK_HOST: "::",
  };
  const timeoutMs = options?.timeoutMs ?? PI_TEXT_GENERATION_TIMEOUT_MS;

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (current, chunk) => current + chunk,
      ),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to collect Pi process output.",
            cause,
          }),
      ),
    );

  const runPiJson = Effect.fn("runPiJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchema: S;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    if (!settings.jarvisProjectPath.trim()) {
      return yield* new TextGenerationError({
        operation,
        detail: "Configure the Jarvis project path before using Pi text generation.",
      });
    }

    const piCliPath = path.join(
      path.resolve(settings.jarvisProjectPath),
      "node_modules",
      "@earendil-works",
      "pi-coding-agent",
      "dist",
      "cli.js",
    );
    const piCliExists = yield* fileSystem.exists(piCliPath).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to inspect the configured Jarvis project.",
            cause,
          }),
      ),
    );
    if (!piCliExists) {
      return yield* new TextGenerationError({
        operation,
        detail: "The configured Jarvis project does not contain the Pi CLI. Run npm install there.",
      });
    }

    const thinkingLevel = selectedThinkingLevel(modelSelection);
    const runCommand = Effect.gen(function* () {
      const command = ChildProcess.make(
        options?.nodePath ?? process.execPath,
        [
          piCliPath,
          "--mode",
          "json",
          "--no-session",
          "--no-extensions",
          "--no-skills",
          "--no-prompt-templates",
          "--no-context-files",
          "--no-tools",
          "--no-approve",
          "--model",
          modelSelection.model,
          ...(thinkingLevel ? ["--thinking", thinkingLevel] : []),
        ],
        {
          cwd,
          env: environment,
          stdin: { stream: Stream.encodeText(Stream.make(prompt)) },
          stdout: "pipe",
          stderr: "pipe",
          killSignal: "SIGTERM",
          forceKillAfter: "2 seconds",
        },
      );
      const child = yield* Effect.acquireRelease(
        commandSpawner.spawn(command).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: "Failed to start Pi text generation.",
                cause,
              }),
          ),
        ),
        (handle) => handle.kill().pipe(Effect.ignore),
      );
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
                  detail: "Failed to read the Pi process exit code.",
                  cause,
                }),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );
      if (exitCode !== 0) {
        const detail = stderr.trim() || stdout.trim();
        return yield* new TextGenerationError({
          operation,
          detail: detail
            ? `Pi text generation failed: ${detail}`
            : `Pi text generation failed with code ${Number(exitCode)}.`,
        });
      }
      return stdout;
    });

    const output = yield* runCommand.pipe(
      Effect.scoped,
      Effect.timeoutOption(Duration.millis(timeoutMs)),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "Pi text generation timed out." }),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );
    const finalMessage = finalAssistantMessage(output);
    if (!finalMessage) {
      return yield* new TextGenerationError({
        operation,
        detail: "Pi did not return a complete assistant message.",
      });
    }
    if (finalMessage.error) {
      return yield* new TextGenerationError({
        operation,
        detail: `Pi text generation failed: ${finalMessage.error}`,
      });
    }
    if (!finalMessage.text) {
      return yield* new TextGenerationError({
        operation,
        detail: "Pi returned an empty assistant message.",
      });
    }

    return yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
      extractJsonObject(finalMessage.text),
    ).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "Pi returned invalid structured output.",
              cause,
            }),
          ),
      }),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizePrTitle(generated.title), body: generated.body.trim() };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizeThreadTitle(generated.title) };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

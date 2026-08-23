// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";

import { makePiTextGeneration } from "./PiTextGeneration.ts";

const fixturePath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "testFixtures",
  "piJsonMockPeer.mjs",
);
const decodeSettings = Schema.decodeSync(PiSettings);
const instanceId = ProviderInstanceId.make("pi-test");

function makeFakeJarvisRoot(): string {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-text-generation-"));
  const cli = NodePath.join(
    root,
    "node_modules",
    "@earendil-works",
    "pi-coding-agent",
    "dist",
    "cli.js",
  );
  NodeFS.mkdirSync(NodePath.dirname(cli), { recursive: true });
  NodeFS.copyFileSync(fixturePath, cli);
  return root;
}

const modelSelection = (model: string) => ({ instanceId, model, options: [] });

describe("PiTextGeneration", () => {
  it.effect("generates all T3 metadata through an isolated Pi process", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const generation = yield* makePiTextGeneration(
        decodeSettings({ jarvisProjectPath: jarvisRoot }),
      );
      const selection = modelSelection("mock/success");

      expect(
        yield* generation.generateCommitMessage({
          cwd: jarvisRoot,
          branch: "main",
          stagedSummary: "M file.ts",
          stagedPatch: "+change",
          includeBranch: true,
          modelSelection: selection,
        }),
      ).toEqual({
        subject: "Fix Pi metadata",
        body: "Explain the generated metadata.",
        branch: "feature/pi-metadata",
      });

      expect(
        yield* generation.generatePrContent({
          cwd: jarvisRoot,
          baseBranch: "main",
          headBranch: "feature/pi-metadata",
          commitSummary: "Fix metadata",
          diffSummary: "M file.ts",
          diffPatch: "+change",
          modelSelection: selection,
        }),
      ).toEqual({
        title: "Generate Pi metadata",
        body: "Explain the generated metadata.",
      });

      expect(
        yield* generation.generateBranchName({
          cwd: jarvisRoot,
          message: "Generate metadata",
          modelSelection: selection,
        }),
      ).toEqual({ branch: "feature/pi-metadata" });

      expect(
        yield* generation.generateThreadTitle({
          cwd: jarvisRoot,
          message: "Generate metadata",
          modelSelection: selection,
        }),
      ).toEqual({ title: "Generate Pi metadata" });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("recovers after malformed and failed Pi metadata processes", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const generation = yield* makePiTextGeneration(
        decodeSettings({ jarvisProjectPath: jarvisRoot }),
      );
      const input = {
        cwd: jarvisRoot,
        message: "Generate metadata",
      };

      const malformed = yield* Effect.flip(
        generation.generateThreadTitle({
          ...input,
          modelSelection: modelSelection("mock/malformed"),
        }),
      );
      expect(malformed).toMatchObject({
        _tag: "TextGenerationError",
        detail: "Pi returned invalid structured output.",
      });

      const failed = yield* Effect.flip(
        generation.generateThreadTitle({
          ...input,
          modelSelection: modelSelection("mock/exit"),
        }),
      );
      expect(failed).toMatchObject({
        _tag: "TextGenerationError",
        detail: expect.stringContaining("mock authentication failed"),
      });

      const providerError = yield* Effect.flip(
        generation.generateThreadTitle({
          ...input,
          modelSelection: modelSelection("mock/provider-error"),
        }),
      );
      expect(providerError).toMatchObject({
        _tag: "TextGenerationError",
        detail: "Pi text generation failed: mock provider unavailable",
      });

      expect(
        yield* generation.generateThreadTitle({
          ...input,
          modelSelection: modelSelection("mock/success"),
        }),
      ).toEqual({ title: "Generate Pi metadata" });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("times out Pi metadata generation and releases the child process", () =>
    Effect.gen(function* () {
      const jarvisRoot = makeFakeJarvisRoot();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(jarvisRoot, { recursive: true, force: true })),
      );
      const generation = yield* makePiTextGeneration(
        decodeSettings({ jarvisProjectPath: jarvisRoot }),
        { timeoutMs: 25 },
      );

      const failure = yield* Effect.flip(
        generation.generateThreadTitle({
          cwd: jarvisRoot,
          message: "Hang",
          modelSelection: modelSelection("mock/hang"),
        }),
      );
      expect(failure).toMatchObject({
        _tag: "TextGenerationError",
        detail: "Pi text generation timed out.",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

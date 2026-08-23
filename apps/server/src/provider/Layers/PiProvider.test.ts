// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";

import { checkPiProviderStatus } from "./PiProvider.ts";

const fixturePath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "..",
  "pi",
  "testFixtures",
  "piRpcMockPeer.mjs",
);
const decodeSettings = Schema.decodeSync(PiSettings);

describe("PiProvider", () => {
  it.effect("probes the Jarvis extension and discovers Pi models", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-provider-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(root, { recursive: true, force: true })),
      );
      const cli = NodePath.join(
        root,
        "node_modules",
        "@earendil-works",
        "pi-coding-agent",
        "dist",
        "cli.js",
      );
      const extension = NodePath.join(root, "src", "extension.ts");
      NodeFS.mkdirSync(NodePath.dirname(cli), { recursive: true });
      NodeFS.mkdirSync(NodePath.dirname(extension), { recursive: true });
      NodeFS.copyFileSync(fixturePath, cli);
      NodeFS.writeFileSync(extension, "export default function jarvis() {}\n");
      NodeFS.writeFileSync(NodePath.join(root, "package.json"), '{"version":"0.1.0"}\n');

      const snapshot = yield* checkPiProviderStatus(decodeSettings({ jarvisProjectPath: root }));
      expect(snapshot).toMatchObject({
        displayName: "Jarvis (Pi)",
        status: "ready",
        installed: true,
        version: "0.1.0",
        auth: { status: "authenticated" },
      });
      expect(snapshot.models).toEqual([
        expect.objectContaining({ slug: "openai-codex/gpt-test", name: "GPT Test" }),
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

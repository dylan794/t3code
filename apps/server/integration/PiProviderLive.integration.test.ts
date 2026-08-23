// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings, ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { makePiAdapter } from "../src/provider/Layers/PiAdapter.ts";
import { makePiTextGeneration } from "../src/textGeneration/PiTextGeneration.ts";

const JARVIS_ROOT =
  process.env.JARVIS_PROJECT_PATH ??
  NodePath.resolve(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    "..",
    "..",
    "..",
    "..",
    "jarvis_project",
  );
const INSTANCE_ID = ProviderInstanceId.make("pi-live-local");
const MODEL = "t3-live-local/mock";
const decodeSettings = Schema.decodeSync(PiSettings);

interface LocalModelEndpoint {
  readonly baseUrl: string;
  readonly requestCount: () => number;
}

function startLocalModelEndpoint(): Promise<{
  readonly endpoint: LocalModelEndpoint;
  readonly close: () => Promise<void>;
}> {
  let requests = 0;
  const server = NodeHttp.createServer((request, response) => {
    const body: Buffer[] = [];
    request.on("data", (chunk: Buffer) => body.push(chunk));
    request.on("end", () => {
      requests += 1;
      try {
        JSON.parse(Buffer.concat(body).toString("utf8"));
      } catch {
        response.writeHead(400).end();
        return;
      }

      const text =
        requests === 1 ? JSON.stringify({ title: "Offline Pi metadata" }) : "Offline Pi reply";
      response.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      response.write(
        `data: ${JSON.stringify({
          id: `local-${requests}`,
          object: "chat.completion.chunk",
          created: 0,
          model: "mock",
          choices: [{ index: 0, delta: { role: "assistant", content: text }, finish_reason: null }],
        })}\n\n`,
      );
      response.write(
        `data: ${JSON.stringify({
          id: `local-${requests}`,
          object: "chat.completion.chunk",
          created: 0,
          model: "mock",
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
        })}\n\n`,
      );
      response.end("data: [DONE]\n\n");
    });
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("Local model endpoint did not bind a TCP port."));
        return;
      }
      resolve({
        endpoint: {
          baseUrl: `http://127.0.0.1:${address.port}/v1`,
          requestCount: () => requests,
        },
        close: () =>
          new Promise((closeResolve, closeReject) =>
            server.close((error) => (error ? closeReject(error) : closeResolve())),
          ),
      });
    });
  });
}

function writeLocalPiProfile(profileDir: string, baseUrl: string): void {
  NodeFS.mkdirSync(profileDir, { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(profileDir, "models.json"),
    JSON.stringify({
      providers: {
        "t3-live-local": {
          baseUrl,
          api: "openai-completions",
          apiKey: "local-test-key",
          compat: {
            supportsDeveloperRole: false,
            supportsReasoningEffort: false,
          },
          models: [
            {
              id: "mock",
              name: "T3 live local model",
              reasoning: false,
              input: ["text"],
              contextWindow: 32_000,
              maxTokens: 2_000,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            },
          ],
        },
      },
    }),
  );
}

describe("Pi provider live integration", () => {
  it.live.skipIf(!NodeFS.existsSync(JARVIS_ROOT))(
    "generates metadata, rolls back, and resumes through installed Pi",
    () =>
      Effect.gen(function* () {
        const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-live-"));
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(tempRoot, { recursive: true, force: true })),
        );
        const runningEndpoint = yield* Effect.acquireRelease(
          Effect.promise(startLocalModelEndpoint),
          ({ close }) => Effect.promise(close).pipe(Effect.ignore),
        );
        const profileDir = NodePath.join(tempRoot, "pi-profile");
        const workspace = NodePath.join(tempRoot, "workspace");
        NodeFS.mkdirSync(workspace, { recursive: true });
        writeLocalPiProfile(profileDir, runningEndpoint.endpoint.baseUrl);

        const environment = { ...process.env, PI_CODING_AGENT_DIR: profileDir };
        const settings = decodeSettings({
          jarvisProjectPath: JARVIS_ROOT,
          launchArgs:
            "--no-skills --no-prompt-templates --no-context-files --no-tools --no-approve",
        });
        const selection = { instanceId: INSTANCE_ID, model: MODEL, options: [] };
        const generation = yield* makePiTextGeneration(settings, {
          environment,
          timeoutMs: 30_000,
        });
        expect(
          yield* generation.generateThreadTitle({
            cwd: workspace,
            message: "Verify installed Pi metadata generation",
            modelSelection: selection,
          }),
        ).toEqual({ title: "Offline Pi metadata" });

        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment,
        });
        const threadId = ThreadId.make("pi-live-local-thread");
        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("pi"),
          providerInstanceId: INSTANCE_ID,
          cwd: workspace,
          runtimeMode: "full-access",
          modelSelection: selection,
        });

        for (const input of ["first turn", "second turn"]) {
          const completed = yield* adapter.streamEvents.pipe(
            Stream.takeUntil((event) => event.type === "turn.completed"),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* Effect.yieldNow;
          yield* adapter.sendTurn({ threadId, input, modelSelection: selection });
          expect(Array.from(yield* Fiber.join(completed))).toContainEqual(
            expect.objectContaining({ type: "turn.completed" }),
          );
        }

        const rolledBack = yield* adapter.rollbackThread(threadId, 1);
        expect(rolledBack.turns).toHaveLength(1);
        expect(rolledBack.resumeCursor).toMatchObject({ schemaVersion: 1 });
        yield* adapter.stopSession(threadId);

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("pi"),
          providerInstanceId: INSTANCE_ID,
          cwd: workspace,
          runtimeMode: "full-access",
          modelSelection: selection,
          resumeCursor: rolledBack.resumeCursor,
        });
        const recovered = yield* adapter.streamEvents.pipe(
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* adapter.sendTurn({ threadId, input: "after recovery", modelSelection: selection });
        const recoveredEvents = Array.from(yield* Fiber.join(recovered));
        expect(recoveredEvents).toContainEqual(
          expect.objectContaining({
            type: "content.delta",
            payload: expect.objectContaining({ delta: "Offline Pi reply" }),
          }),
        );
        expect(runningEndpoint.endpoint.requestCount()).toBeGreaterThanOrEqual(4);
        yield* adapter.stopSession(threadId);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

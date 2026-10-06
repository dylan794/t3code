// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  authenticateJarvisOwner,
  mayPresentJarvisOwner,
  revokeJarvisOwner,
} from "./JarvisOwnerCore.ts";
import { ownerPrivateChannel } from "./OwnerPrivateChannel.ts";

describe("Jarvis Owner Core callback", () => {
  it("keeps the grant outside client frames, verifies exact presentation, and rejects a revoked late result", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-owner-core-"));
    const previous = process.env.JARVIS_SERVICE_DIR;
    process.env.JARVIS_SERVICE_DIR = root;
    const frames: unknown[] = [];
    const thread = ThreadId.make("owner-core-test");
    const disconnect = ownerPrivateChannel.connect("owner-core-client");
    try {
      await NodeFSP.writeFile(
        NodePath.join(root, "endpoint.json"),
        JSON.stringify({ origin: "http://127.0.0.1:12345" }),
      );
      ownerPrivateChannel.subscribe("owner-core-client", (frame) => {
        frames.push(frame);
        if (frame.kind === "conceal")
          ownerPrivateChannel.acknowledge("owner-core-client", frame.threadId, frame.epoch);
      });
      ownerPrivateChannel.claim(thread, "owner-core-client");
      await ownerPrivateChannel.conceal(thread);
      const scope = {
        project: "/project",
        provider: "provider",
        model: "model",
        endpoint: "https://model.example/",
      };
      const binding = {
        hostConnectionId: "host",
        environment: "environment",
        thread,
        clientId: "owner-core-client",
      };
      const presentation = {
        allowed: true,
        environment: "environment",
        thread,
        destination: scope,
        expiresAt: new Date(Date.now() + 30_000).toISOString(),
      };
      const fetcher = vi.fn(async (_url: string, init: RequestInit) => {
        const body = JSON.parse(String(init.body));
        if ("password" in body)
          return new Response(
            JSON.stringify({ token: "TRANSPORT-ONLY-TOKEN", expiresAt: presentation.expiresAt }),
          );
        return new Response(JSON.stringify(presentation));
      });
      vi.stubGlobal("fetch", fetcher);
      expect(await authenticateJarvisOwner(thread, "PRIVATE-PASSWORD", scope, binding)).toBe(true);
      expect(await mayPresentJarvisOwner(thread, "other-client")).toBe(false);
      expect(JSON.stringify(frames)).not.toContain("PRIVATE-PASSWORD");
      expect(JSON.stringify(frames)).not.toContain("TRANSPORT-ONLY-TOKEN");
      fetcher.mockImplementationOnce(async () => {
        revokeJarvisOwner(thread);
        return new Response(JSON.stringify(presentation));
      });
      expect(await mayPresentJarvisOwner(thread, "owner-core-client")).toBe(false);
      expect(await mayPresentJarvisOwner(thread, "owner-core-client")).toBe(false);
      expect(await authenticateJarvisOwner(thread, "PRIVATE-PASSWORD", scope, binding)).toBe(true);
      const started = Promise.withResolvers<void>();
      const delayed = Promise.withResolvers<Response>();
      fetcher.mockImplementationOnce(() => {
        started.resolve();
        return delayed.promise;
      });
      const oldPresentation = mayPresentJarvisOwner(thread, binding.clientId);
      await started.promise;
      expect(await authenticateJarvisOwner(thread, "NEW-PASSWORD", scope, binding)).toBe(true);
      delayed.resolve(new Response(JSON.stringify({ allowed: false })));
      expect(await oldPresentation).toBe(false);
      expect(await mayPresentJarvisOwner(thread, binding.clientId)).toBe(true);

      const authenticationStarted = Promise.withResolvers<void>();
      const authenticationReply = Promise.withResolvers<Response>();
      fetcher.mockImplementationOnce(() => {
        authenticationStarted.resolve();
        return authenticationReply.promise;
      });
      const oldAuthentication = authenticateJarvisOwner(thread, "OLD-PASSWORD", scope, binding);
      await authenticationStarted.promise;
      expect(await authenticateJarvisOwner(thread, "NEW-PASSWORD", scope, binding)).toBe(true);
      authenticationReply.resolve(
        new Response(
          JSON.stringify({
            token: "OLD-TOKEN",
            expiresAt: presentation.expiresAt,
          }),
        ),
      );
      expect(await oldAuthentication).toBe(false);
      expect(await mayPresentJarvisOwner(thread, binding.clientId)).toBe(true);
      const concealCount = frames.filter(
        (frame) => (frame as { kind: string }).kind === "conceal",
      ).length;
      const expiry = Date.parse(presentation.expiresAt) + 1;
      const clock = vi.spyOn(Date, "now").mockReturnValue(expiry);
      try {
        expect(await mayPresentJarvisOwner(thread, binding.clientId)).toBe(false);
        expect(
          frames.filter((frame) => (frame as { kind: string }).kind === "conceal").length,
        ).toBeGreaterThan(concealCount);
      } finally {
        clock.mockRestore();
      }
    } finally {
      revokeJarvisOwner(thread);
      disconnect();
      vi.unstubAllGlobals();
      if (previous === undefined) delete process.env.JARVIS_SERVICE_DIR;
      else process.env.JARVIS_SERVICE_DIR = previous;
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  });
});

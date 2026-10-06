// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalFetch:off
// @effect-diagnostics globalDate:off
// @effect-diagnostics globalTimers:off
/* eslint-disable t3code/no-global-process-runtime -- Promise adapter resolves the local credential directory. */
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { ThreadId } from "@t3tools/contracts";
import { ownerPrivateChannel } from "./OwnerPrivateChannel.ts";

export interface JarvisScope {
  project: string;
  provider: string;
  model: string;
  endpoint: string;
}
interface Binding {
  hostConnectionId: string;
  environment: string;
  thread: string;
  clientId: string;
}
interface Grant {
  token: string;
  scope: JarvisScope;
  binding: Binding;
  expiresAt: string;
  epoch: number;
}
const grants = new Map<ThreadId, Grant>();
const authentications = new Map<ThreadId, symbol>();
const monitors = new Map<ThreadId, ReturnType<typeof setInterval>>();

function serviceDirectory(): string {
  if (process.env.JARVIS_SERVICE_DIR) return NodePath.resolve(process.env.JARVIS_SERVICE_DIR);
  // This Promise-based loopback adapter uses the local machine's credential directory.
  const root =
    NodeOS.platform() === "win32"
      ? process.env.LOCALAPPDATA || NodePath.join(NodeOS.homedir(), "AppData", "Local")
      : process.env.XDG_CONFIG_HOME || NodePath.join(NodeOS.homedir(), ".config");
  return NodePath.join(root, "Jarvis", "service");
}
async function request(path: string, input: unknown, token?: string): Promise<unknown> {
  const endpoint: unknown = JSON.parse(
    await NodeFSP.readFile(NodePath.join(serviceDirectory(), "endpoint.json"), "utf8"),
  );
  if (
    !endpoint ||
    typeof endpoint !== "object" ||
    !("origin" in endpoint) ||
    typeof endpoint.origin !== "string"
  )
    throw new Error("Owner Core unavailable");
  const url = new URL(endpoint.origin);
  if (
    url.protocol !== "http:" ||
    url.hostname !== "127.0.0.1" ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  )
    throw new Error("Owner Core unavailable");
  const response = await fetch(`${url.origin}/v1/memory/host/${path}`, {
    method: "POST",
    redirect: "error",
    signal: AbortSignal.timeout(2000),
    headers: {
      "Content-Type": "application/json",
      Origin: url.origin,
      ...(token ? { "X-Jarvis-Memory": token } : {}),
    },
    body: JSON.stringify(input),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error("Owner Core unavailable");
  }
  return response.json();
}
function object(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
export function revokeJarvisOwner(threadId: ThreadId): void {
  authentications.delete(threadId);
  grants.delete(threadId);
  const monitor = monitors.get(threadId);
  if (monitor) clearInterval(monitor);
  monitors.delete(threadId);
  ownerPrivateChannel.lock(threadId);
}
export function monitorJarvisOwner(threadId: ThreadId, clientId: string): void {
  if (monitors.has(threadId)) return;
  let running = false;
  const monitor = setInterval(() => {
    if (running) return;
    running = true;
    void mayPresentJarvisOwner(threadId, clientId).finally(() => {
      running = false;
    });
  }, 1000);
  monitor.unref();
  monitors.set(threadId, monitor);
}
export async function authenticateJarvisOwner(
  threadId: ThreadId,
  password: string,
  scope: JarvisScope,
  binding: Binding,
): Promise<boolean> {
  grants.delete(threadId);
  const authentication = Symbol();
  authentications.set(threadId, authentication);
  const epoch = ownerPrivateChannel.epoch(threadId);
  try {
    const reply = object(
      await request("authenticate", { password, confirmed: true, scope, ...binding }),
    );
    if (
      !reply ||
      typeof reply.token !== "string" ||
      typeof reply.expiresAt !== "string" ||
      !(Date.parse(reply.expiresAt) > Date.now()) ||
      Date.parse(reply.expiresAt) > Date.now() + 600_000 ||
      authentications.get(threadId) !== authentication ||
      ownerPrivateChannel.clientFor(threadId) !== binding.clientId ||
      ownerPrivateChannel.epoch(threadId) !== epoch
    )
      return false;
    grants.set(threadId, {
      token: reply.token,
      expiresAt: reply.expiresAt,
      scope: { ...scope },
      binding: { ...binding },
      epoch,
    });
    return await mayPresentJarvisOwner(threadId, binding.clientId);
  } catch {
    return false;
  } finally {
    if (authentications.get(threadId) === authentication) authentications.delete(threadId);
  }
}
export async function mayPresentJarvisOwner(
  threadId: ThreadId,
  clientId: string,
): Promise<boolean> {
  const grant = grants.get(threadId);
  if (!grant || grant.binding.clientId !== clientId) return false;
  if (
    !(Date.parse(grant.expiresAt) > Date.now()) ||
    grant.epoch !== ownerPrivateChannel.epoch(threadId) ||
    !ownerPrivateChannel.acknowledged(threadId, clientId)
  ) {
    revokeJarvisOwner(threadId);
    return false;
  }
  try {
    const reply = object(
      await request(
        "presentation",
        {
          ...grant.binding,
          provider: grant.scope.provider,
          model: grant.scope.model,
          endpoint: grant.scope.endpoint,
        },
        grant.token,
      ),
    );
    const destination = object(reply?.destination);
    const allowed =
      reply?.allowed === true &&
      reply.environment === grant.binding.environment &&
      reply.thread === threadId &&
      typeof reply.expiresAt === "string" &&
      Date.parse(reply.expiresAt) > Date.now() &&
      Date.parse(reply.expiresAt) <= Date.now() + 60_000 &&
      Object.entries(grant.scope).every(([key, value]) => destination?.[key] === value) &&
      grants.get(threadId) === grant &&
      ownerPrivateChannel.acknowledged(threadId, clientId) &&
      grant.epoch === ownerPrivateChannel.epoch(threadId);
    if (!allowed && grants.get(threadId) === grant) revokeJarvisOwner(threadId);
    return allowed;
  } catch {
    if (grants.get(threadId) === grant) revokeJarvisOwner(threadId);
    return false;
  }
}

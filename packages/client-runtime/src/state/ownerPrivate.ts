import {
  WS_METHODS,
  type EnvironmentId,
  type ThreadId,
  OwnerPrivateFrame,
  OwnerPrivateResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { Atom } from "effect/unstable/reactivity";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentCacheStore, type ConnectionPersistenceError } from "../platform/persistence.ts";
import { request, subscribeDynamic } from "../rpc/client.ts";
import { createEnvironmentCommand, createEnvironmentSubscriptionAtomFamily } from "./runtime.ts";

type Frame = typeof OwnerPrivateFrame.Type;
type Response = typeof OwnerPrivateResponse.Type;
export type PrivatePresentation = Frame & { readonly environmentId: EnvironmentId };
const locks = new Map<string, number>();
const volatileThreads = new Set<string>();
const pendingFrames = new Map<string, PrivatePresentation>();
const listeners = new Set<() => void>();
const purgers = new Map<string, Set<Effect.Effect<void, ConnectionPersistenceError>>>();
const requiredPurges = new Map<
  EnvironmentId,
  ReadonlyArray<Effect.Effect<void, ConnectionPersistenceError>>
>();
let presentation: PrivatePresentation | null = null;
const key = (environmentId: EnvironmentId, threadId: ThreadId) =>
  JSON.stringify([environmentId, threadId]);

export function isOwnerPrivateConcealed(
  environmentId: EnvironmentId,
  threadId?: ThreadId,
): boolean {
  return threadId === undefined
    ? Array.from(locks.keys()).some((value) => JSON.parse(value)[0] === environmentId)
    : locks.has(key(environmentId, threadId));
}
export function isOwnerPrivateVolatile(environmentId: EnvironmentId, threadId?: ThreadId): boolean {
  return threadId === undefined
    ? Array.from(volatileThreads).some((value) => JSON.parse(value)[0] === environmentId)
    : volatileThreads.has(key(environmentId, threadId));
}
export const ownerPrivatePresentation = {
  getSnapshot: () => presentation,
  subscribe: (listener: () => void) => {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  },
};
export function receiveOwnerPrivateFrame(environmentId: EnvironmentId, frame: Frame): void {
  const identity = key(environmentId, frame.threadId);
  const epoch = locks.get(identity);
  if (epoch !== undefined && frame.epoch < epoch) return;
  if (frame.kind === "restore") {
    if (
      epoch === undefined ||
      epoch !== frame.epoch ||
      pendingFrames.get(identity)?.requestId !== frame.requestId
    )
      return;
    locks.delete(identity);
    pendingFrames.delete(identity);
    presentation = Array.from(pendingFrames.values()).at(-1) ?? null;
    for (const listener of listeners) listener();
    return;
  }
  volatileThreads.add(identity);
  // Cancellation never removes the lock. Only a fresh trusted server session
  // can grant a new presentation; model output cannot reveal cached content.
  locks.set(identity, frame.epoch);
  presentation = { ...frame, environmentId };
  pendingFrames.set(identity, presentation);
  for (const listener of listeners) listener();
}
export function revokeOwnerPrivatePresentation(environmentId: EnvironmentId): void {
  for (const identity of volatileThreads) {
    const [environment, threadId] = JSON.parse(identity) as [EnvironmentId, ThreadId];
    if (environment !== environmentId) continue;
    const epoch = locks.get(identity) ?? 0;
    locks.set(identity, epoch);
    presentation = { environmentId, threadId, kind: "cancel", epoch, requestId: "disconnected" };
    pendingFrames.set(identity, presentation);
  }
  for (const listener of listeners) listener();
}
export function beginOwnerPrivateSession(environmentId: EnvironmentId): void {
  revokeOwnerPrivatePresentation(environmentId);
  for (const identity of volatileThreads) {
    if ((JSON.parse(identity) as [EnvironmentId, ThreadId])[0] !== environmentId) continue;
    locks.set(identity, 0);
    const pending = pendingFrames.get(identity);
    if (pending) pendingFrames.set(identity, { ...pending, epoch: 0, requestId: "disconnected" });
  }
  requiredPurges.delete(environmentId);
}
export function registerOwnerPrivatePurger(
  environmentId: EnvironmentId,
  purge: Effect.Effect<void, ConnectionPersistenceError>,
): () => void {
  const set = purgers.get(environmentId) ?? new Set();
  set.add(purge);
  purgers.set(environmentId, set);
  return () => {
    set.delete(purge);
    if (!set.size) purgers.delete(environmentId);
  };
}

export function createOwnerPrivateAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | EnvironmentCacheStore | R, E>,
) {
  const frames = createEnvironmentSubscriptionAtomFamily(runtime, {
    label: "owner-private-channel",
    idleTtlMs: 0,
    subscribe: (input: { readonly environmentId: EnvironmentId }) =>
      subscribeDynamic(
        WS_METHODS.ownerPrivateSubscribe,
        () =>
          Effect.sync(() => {
            beginOwnerPrivateSession(input.environmentId);
            return {};
          }),
        {
          onSubscriptionEnd: Effect.sync(() => revokeOwnerPrivatePresentation(input.environmentId)),
        },
      ).pipe(
        Stream.tap((frame) =>
          Effect.gen(function* () {
            // Capture cleanup handles before notifying React. Removing the
            // ordinary subtree must not drop the only reference to cached state.
            const cleanup = Array.from(purgers.get(input.environmentId) ?? []);
            if (frame.kind === "conceal") requiredPurges.set(input.environmentId, cleanup);
            receiveOwnerPrivateFrame(input.environmentId, frame);
            if (frame.kind === "conceal" || frame.kind === "cancel") {
              for (const purge of cleanup) yield* purge;
            }
          }),
        ),
        Stream.ensuring(Effect.sync(() => revokeOwnerPrivatePresentation(input.environmentId))),
      ),
  });
  const respond = createEnvironmentCommand(runtime, {
    label: "owner-private-response",
    execute: (input: Response, _registry, environmentId) =>
      Effect.gen(function* () {
        if (locks.get(key(environmentId, input.threadId)) !== input.epoch)
          return { accepted: false };
        if (input.kind === "ack") {
          // The UI calls this only after committing a tree without ordinary
          // content. A failed cleanup prevents the server acknowledgement.
          for (const purge of requiredPurges.get(environmentId) ?? purgers.get(environmentId) ?? [])
            yield* purge;
          const cache = yield* EnvironmentCacheStore;
          yield* cache.clear(environmentId);
        }
        return yield* request(WS_METHODS.ownerPrivateRespond, input);
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            Reflect.deleteProperty(input, "value");
          }),
        ),
      ),
  });
  return { frames, respond };
}

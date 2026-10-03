// @effect-diagnostics globalTimers:off
import type { ThreadId } from "@t3tools/contracts";

export type OwnerPrivateFrame = {
  kind: "conceal" | "secret" | "cancel" | "restore";
  threadId: ThreadId;
  requestId: string;
  epoch: number;
};
interface Client {
  deliver?: (frame: OwnerPrivateFrame) => void;
  acknowledged: Map<ThreadId, number>;
}
interface Pending {
  threadId: ThreadId;
  clientId: string;
  epoch: number;
  resolve: (value: string | undefined) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** Dedicated connection-local transport. No frame or receipt contains a secret. */
export class OwnerPrivateChannel {
  private clients = new Map<string, Client>();
  private threads = new Map<
    ThreadId,
    { epoch: number; ownerClient?: string; restoreRequestId?: string }
  >();
  private pending = new Map<string, Pending>();
  private acknowledgments = new Map<
    ThreadId,
    { resolve: (value: boolean) => void; timer: ReturnType<typeof setTimeout> }
  >();
  connect(clientId: string): () => void {
    if (this.clients.size >= 256 || this.clients.has(clientId))
      throw new Error("Private channel unavailable");
    this.clients.set(clientId, { acknowledged: new Map() });
    return () => {
      this.clients.delete(clientId);
      for (const [threadId, state] of this.threads) {
        if (state.ownerClient === clientId) {
          this.lock(threadId);
          delete state.ownerClient;
        }
        this.completeAcknowledgment(threadId);
      }
    };
  }
  subscribe(clientId: string, deliver: (frame: OwnerPrivateFrame) => void): () => void {
    const client = this.clients.get(clientId);
    if (!client || client.deliver) throw new Error("Private channel unavailable");
    client.deliver = deliver;
    for (const [threadId, state] of this.threads)
      deliver({ kind: "conceal", threadId, requestId: "conceal", epoch: state.epoch });
    return () => {
      delete client.deliver;
      client.acknowledged.clear();
      for (const [threadId, state] of this.threads)
        if (state.ownerClient === clientId) this.lock(threadId);
    };
  }
  claim(threadId: ThreadId, clientId: string): void {
    if (!this.clients.get(clientId)?.deliver || this.threads.size >= 1024)
      throw new Error("Private channel unavailable");
    const state = this.threads.get(threadId);
    if (state?.ownerClient && state.ownerClient !== clientId)
      throw new Error("Private channel unavailable");
    this.threads.set(threadId, { epoch: state?.epoch ?? 0, ownerClient: clientId });
  }
  concealed(threadId: ThreadId): boolean {
    return (this.threads.get(threadId)?.epoch ?? 0) > 0;
  }
  clientFor(threadId: ThreadId): string | undefined {
    return this.threads.get(threadId)?.ownerClient;
  }
  epoch(threadId: ThreadId): number {
    return this.threads.get(threadId)?.epoch ?? 0;
  }
  registerPi(threadId: ThreadId): void {
    const state = this.threads.get(threadId);
    this.threads.set(threadId, {
      epoch: Math.max(1, state?.epoch ?? 0),
      ...(state?.ownerClient ? { ownerClient: state.ownerClient } : {}),
    });
  }
  restore(threadId: ThreadId, clientId: string): void {
    const state = this.threads.get(threadId);
    if (state?.ownerClient !== clientId) return;
    this.clients.get(clientId)?.deliver?.({
      kind: "restore",
      threadId,
      requestId: state.restoreRequestId ?? "conceal",
      epoch: state.epoch,
    });
  }
  lock(threadId: ThreadId): void {
    this.cancel(threadId);
    const state = this.threads.get(threadId);
    if (!state) return;
    state.epoch++;
    for (const client of this.clients.values())
      client.deliver?.({ kind: "conceal", threadId, requestId: "conceal", epoch: state.epoch });
  }
  tryClaim(threadId: ThreadId, clientId: string): void {
    try {
      this.claim(threadId, clientId);
    } catch {
      /* No private channel: unlock remains unavailable. */
    }
  }
  acknowledge(clientId: string, threadId: ThreadId, epoch: number): void {
    const client = this.clients.get(clientId),
      state = this.threads.get(threadId);
    if (!client?.deliver || !state || state.epoch !== epoch)
      throw new Error("Private channel unavailable");
    client.acknowledged.set(threadId, epoch);
    this.completeAcknowledgment(threadId);
  }
  async conceal(threadId: ThreadId): Promise<boolean> {
    this.cancel(threadId);
    const previous = this.threads.get(threadId);
    const state = {
      epoch: (previous?.epoch ?? 0) + 1,
      ...(previous?.ownerClient ? { ownerClient: previous.ownerClient } : {}),
    };
    this.threads.set(threadId, state);
    if (!state.ownerClient || !this.clients.get(state.ownerClient)?.deliver) return false;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.acknowledgments.delete(threadId);
        resolve(false);
      }, 4500);
      this.acknowledgments.set(threadId, { resolve, timer });
      for (const client of this.clients.values())
        client.deliver?.({ kind: "conceal", threadId, requestId: "conceal", epoch: state.epoch });
      this.completeAcknowledgment(threadId);
    });
  }
  private completeAcknowledgment(threadId: ThreadId): void {
    const pending = this.acknowledgments.get(threadId),
      state = this.threads.get(threadId);
    if (
      !pending ||
      !state ||
      !state.ownerClient ||
      !this.clients.size ||
      [...this.clients.values()].some(
        (client) => !client.deliver || client.acknowledged.get(threadId) !== state.epoch,
      )
    )
      return;
    clearTimeout(pending.timer);
    this.acknowledgments.delete(threadId);
    pending.resolve(true);
  }
  secret(threadId: ThreadId, requestId: string): Promise<string | undefined> {
    const state = this.threads.get(threadId),
      client = state?.ownerClient ? this.clients.get(state.ownerClient) : undefined;
    if (
      !state?.ownerClient ||
      !client?.deliver ||
      this.pending.has(requestId) ||
      this.pending.size >= 64 ||
      [...this.clients.values()].some(
        (item) => !item.deliver || item.acknowledged.get(threadId) !== state.epoch,
      )
    )
      return Promise.resolve(undefined);
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        resolve(undefined);
        client.deliver?.({ kind: "cancel", threadId, requestId, epoch: state.epoch });
      }, 60_000);
      this.pending.set(requestId, {
        threadId,
        clientId: state.ownerClient!,
        epoch: state.epoch,
        resolve,
        timer,
      });
      client.deliver!({ kind: "secret", threadId, requestId, epoch: state.epoch });
    });
  }
  respond(
    clientId: string,
    threadId: ThreadId,
    requestId: string,
    epoch: number,
    value?: string,
  ): void {
    const pending = this.pending.get(requestId),
      state = this.threads.get(threadId);
    if (
      !pending ||
      pending.threadId !== threadId ||
      pending.clientId !== clientId ||
      state?.ownerClient !== clientId ||
      pending.epoch !== epoch ||
      state.epoch !== epoch ||
      (value !== undefined &&
        (value.length > 256 ||
          [...value].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127)))
    )
      throw new Error("Private channel unavailable");
    clearTimeout(pending.timer);
    this.pending.delete(requestId);
    pending.resolve(value);
    state.restoreRequestId = requestId;
  }
  cancel(threadId: ThreadId): void {
    const ack = this.acknowledgments.get(threadId);
    if (ack) {
      clearTimeout(ack.timer);
      this.acknowledgments.delete(threadId);
      ack.resolve(false);
    }
    for (const [requestId, pending] of this.pending)
      if (pending.threadId === threadId) {
        clearTimeout(pending.timer);
        this.pending.delete(requestId);
        pending.resolve(undefined);
        this.clients
          .get(pending.clientId)
          ?.deliver?.({ kind: "cancel", threadId, requestId, epoch: pending.epoch });
      }
  }
}
export const ownerPrivateChannel = new OwnerPrivateChannel();

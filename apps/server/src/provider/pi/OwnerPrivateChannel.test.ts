import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { OwnerPrivateChannel, type OwnerPrivateFrame } from "./OwnerPrivateChannel.ts";

describe("Owner private channel", () => {
  it("does not conceal an ordinary provider claim on reconnect or owner disconnect", () => {
    const channel = new OwnerPrivateChannel();
    const thread = ThreadId.make("ordinary-provider-thread");
    const disconnect = channel.connect("ordinary-owner");
    channel.subscribe("ordinary-owner", () => {});
    channel.claim(thread, "ordinary-owner");
    const frames: OwnerPrivateFrame[] = [];
    channel.connect("reconnected-client");
    channel.subscribe("reconnected-client", (frame) => frames.push(frame));
    expect(frames).toEqual([]);
    disconnect();
    expect(channel.concealed(thread)).toBe(false);
    expect(frames).toEqual([]);
  });

  it("does not spend private thread capacity on ordinary claims", () => {
    const channel = new OwnerPrivateChannel();
    const disconnect = channel.connect("ordinary-capacity-owner");
    channel.subscribe("ordinary-capacity-owner", () => {});
    for (let index = 0; index < 1024; index++)
      channel.claim(ThreadId.make(`ordinary-capacity-${index}`), "ordinary-capacity-owner");
    expect(() =>
      channel.claim(ThreadId.make("private-capacity"), "ordinary-capacity-owner"),
    ).not.toThrow();
    disconnect();
    channel.connect("next-capacity-owner");
    channel.subscribe("next-capacity-owner", () => {});
    expect(() =>
      channel.claim(ThreadId.make("next-capacity"), "next-capacity-owner"),
    ).not.toThrow();
  });

  it("rejects a secret when a new client has not concealed the current epoch", async () => {
    const channel = new OwnerPrivateChannel();
    const thread = ThreadId.make("late-client-thread");
    channel.connect("late-client-owner");
    channel.subscribe("late-client-owner", (frame) => {
      if (frame.kind === "conceal")
        channel.acknowledge("late-client-owner", frame.threadId, frame.epoch);
    });
    channel.claim(thread, "late-client-owner");
    expect(await channel.conceal(thread)).toBe(true);
    const secret = channel.secret(thread, "late-client-request");
    channel.connect("late-client-unconcealed");
    expect(() =>
      channel.respond(
        "late-client-owner",
        thread,
        "late-client-request",
        channel.epoch(thread),
        "PRIVATE-PASSWORD",
      ),
    ).toThrow();
    channel.lock(thread);
    expect(await secret).toBeUndefined();
  });

  it("requires concealment from every connected client and sends a secret only to the claiming connection", async () => {
    const channel = new OwnerPrivateChannel(),
      thread = ThreadId.make("thread");
    const frames: OwnerPrivateFrame[] = [];
    channel.connect("owner");
    channel.connect("other");
    channel.subscribe("owner", (frame) => {
      frames.push(frame);
      if (frame.kind === "conceal") channel.acknowledge("owner", thread, frame.epoch);
    });
    channel.subscribe("other", (frame) => {
      frames.push(frame);
      if (frame.kind === "conceal") channel.acknowledge("other", thread, frame.epoch);
    });
    channel.claim(thread, "owner");
    expect(await channel.conceal(thread)).toBe(true);
    const secret = channel.secret(thread, "request");
    expect(frames.filter((frame) => frame.kind === "secret")).toHaveLength(1);
    expect(() =>
      channel.respond("other", thread, "request", channel.epoch(thread), "PRIVATE-PASSWORD"),
    ).toThrow();
    channel.respond("owner", thread, "request", channel.epoch(thread), "PRIVATE-PASSWORD");
    expect(await secret).toBe("PRIVATE-PASSWORD");
    expect(JSON.stringify(frames)).not.toContain("PRIVATE-PASSWORD");
    expect(() =>
      channel.respond("owner", thread, "request", channel.epoch(thread), "PRIVATE-PASSWORD"),
    ).toThrow();
  });
  it("fails closed for old clients, expires requests and cancels late secrets on lock", async () => {
    vi.useFakeTimers();
    try {
      const channel = new OwnerPrivateChannel(),
        thread = ThreadId.make("thread");
      channel.connect("owner");
      const disconnectOld = channel.connect("old");
      channel.subscribe("owner", (frame) => {
        if (frame.kind === "conceal") channel.acknowledge("owner", thread, frame.epoch);
      });
      channel.claim(thread, "owner");
      const conceal = channel.conceal(thread);
      await vi.advanceTimersByTimeAsync(4500);
      expect(await conceal).toBe(false);
      disconnectOld();
      expect(await channel.conceal(thread)).toBe(true);
      const epoch = channel.epoch(thread),
        secret = channel.secret(thread, "request");
      channel.lock(thread);
      expect(await secret).toBeUndefined();
      expect(() => channel.respond("owner", thread, "request", epoch, "late-password")).toThrow();
    } finally {
      vi.useRealTimers();
    }
  });
});

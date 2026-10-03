import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { OwnerPrivateChannel, type OwnerPrivateFrame } from "./OwnerPrivateChannel.ts";

describe("Owner private channel", () => {
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

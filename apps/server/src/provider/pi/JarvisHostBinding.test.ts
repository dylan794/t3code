import { describe, expect, it } from "vite-plus/test";
import { jarvisHostEnvironment } from "./JarvisHostBinding.ts";

describe("Jarvis host binding", () => {
  it("keeps enrolled identity stable across process restarts and distinguishes environments, instances and threads", () => {
    const binding = jarvisHostEnvironment("windows-owner", "pi", "thread-1");
    expect(binding).toEqual(jarvisHostEnvironment("windows-owner", "pi", "thread-1"));
    expect(binding.JARVIS_T3_HOST_CONNECTION_ID).toMatch(/^[a-f0-9]{64}$/);
    for (const args of [
      ["mac-owner", "pi", "thread-1"],
      ["windows-owner", "pi-other", "thread-1"],
      ["windows-owner", "pi", "thread-2"],
    ] as const) {
      expect(
        jarvisHostEnvironment(args[0], args[1], args[2]).JARVIS_T3_HOST_CONNECTION_ID,
      ).not.toBe(binding.JARVIS_T3_HOST_CONNECTION_ID);
    }
    expect(Object.keys(binding)).toHaveLength(3);
  });

  it("clears inherited host claims when an explicit environment or canonical thread is unavailable", () => {
    for (const environment of [undefined, "", "../windows", "owner\nforged", "x".repeat(181)]) {
      expect(Object.values(jarvisHostEnvironment(environment, "pi", "thread-1"))).toEqual([
        "",
        "",
        "",
      ]);
    }
    expect(Object.values(jarvisHostEnvironment("windows", "pi", "invalid/thread"))).toEqual([
      "",
      "",
      "",
    ]);
  });
});

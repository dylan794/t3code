import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";
import { deriveProviderInstanceConfigMap } from "./Layers/ProviderInstanceRegistryHydration.ts";

describe("builtInDrivers", () => {
  it("registers Pi without synthesizing a broken legacy default instance", () => {
    expect(BUILT_IN_DRIVERS.map((driver) => driver.driverKind)).toContain("pi");
    expect(deriveProviderInstanceConfigMap(DEFAULT_SERVER_SETTINGS)).not.toHaveProperty("pi");
  });
});

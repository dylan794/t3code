import { PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { PROVIDER_CLIENT_DEFINITION_BY_VALUE } from "./providerDriverMeta";

describe("providerDriverMeta", () => {
  it("exposes Jarvis (Pi) through the generic provider-instance settings UI", () => {
    expect(PROVIDER_CLIENT_DEFINITION_BY_VALUE[ProviderDriverKind.make("pi")]).toMatchObject({
      label: "Jarvis (Pi)",
      settingsSchema: PiSettings,
    });
  });
});

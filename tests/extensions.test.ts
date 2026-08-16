import { describe, expect, it } from "vitest";
import { ExtensionCatalog } from "../src/extensions.js";

describe("resource extension lifecycle", () => {
  it("probes, gates execution and records transitions", () => {
    const catalog = new ExtensionCatalog([
      {
        providerId: "resource-provider.test",
        family: "resource-provider",
        version: "1.0.0",
        contractVersion: "1.0",
        displayName: "Test Resource Provider",
        isolation: "worker",
        capabilities: { store: true },
      },
    ]);
    expect(catalog.probe("resource-provider.test").status).toBe("ready");
    catalog.transition("resource-provider.test", "disabled");
    expect(() => catalog.require("resource-provider.test")).toThrow("disabled");
    expect(() =>
      catalog.transition("resource-provider.test", "canary"),
    ).toThrow("Cannot move");
    expect(catalog.logs("resource-provider.test")).toHaveLength(2);
  });
});

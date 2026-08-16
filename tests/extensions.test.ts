import { describe, expect, it } from "vitest";
import {
  ExtensionCatalog,
  MemoryExtensionStateRepository,
  type ExtensionDescriptor,
} from "../src/extensions.js";

const descriptor: ExtensionDescriptor = {
  providerId: "resource-provider.test",
  family: "resource-provider",
  version: "1.0.0",
  contractVersion: "1.0",
  displayName: "Test Resource Provider",
  isolation: "worker",
  capabilities: { store: true },
};

describe("resource extension lifecycle", () => {
  it("persists probes, lifecycle gates and logs across catalog restarts", async () => {
    const repository = new MemoryExtensionStateRepository();
    const catalog = new ExtensionCatalog([descriptor], repository);
    await catalog.initialize();

    expect((await catalog.probe("resource-provider.test")).status).toBe(
      "ready",
    );
    await catalog.transition("resource-provider.test", "disabled");
    expect(() => catalog.require("resource-provider.test")).toThrow("disabled");
    await expect(
      catalog.transition("resource-provider.test", "canary"),
    ).rejects.toThrow("Cannot move");

    const restored = new ExtensionCatalog([descriptor], repository);
    await restored.initialize();
    expect(restored.get("resource-provider.test").lifecycleState).toBe(
      "disabled",
    );
    expect(await restored.logs("resource-provider.test")).toHaveLength(3);

    expect((await restored.probe("resource-provider.test")).status).toBe(
      "ready",
    );
    await restored.transition("resource-provider.test", "verified");
    expect(restored.get("resource-provider.test").lifecycleState).toBe(
      "verified",
    );
    expect(await restored.logs("resource-provider.test")).toHaveLength(6);
  });
});

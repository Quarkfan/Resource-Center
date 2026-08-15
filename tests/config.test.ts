import { describe, expect, it } from "vitest";
import { requireInternalServiceToken } from "../src/config.js";

describe("service configuration", () => {
  it("requires a strong internal token", () => {
    expect(() => requireInternalServiceToken({})).toThrow(
      "at least 32 characters",
    );
    expect(
      requireInternalServiceToken({ INTERNAL_SERVICE_TOKEN: "x".repeat(32) }),
    ).toHaveLength(32);
  });
});

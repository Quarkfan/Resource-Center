export type ExtensionState =
  | "installed"
  | "verified"
  | "canary"
  | "active"
  | "draining"
  | "disabled"
  | "failed"
  | "retired";
export type ExtensionDescriptor = {
  providerId: string;
  family: string;
  version: string;
  contractVersion: string;
  displayName: string;
  isolation: "in-process" | "worker" | "process" | "container" | "remote";
  capabilities: Record<string, boolean | string | number>;
};
type Entry = {
  descriptor: ExtensionDescriptor;
  lifecycleState: ExtensionState;
  lastProbe?: {
    status: "ready" | "unavailable";
    checkedAt: string;
    reason?: string;
  };
};
const transitions: Record<ExtensionState, ExtensionState[]> = {
  installed: ["verified", "disabled", "retired"],
  verified: ["canary", "active", "disabled", "retired"],
  canary: ["active", "draining", "disabled", "failed"],
  active: ["draining", "disabled", "failed"],
  draining: ["active", "disabled", "retired"],
  disabled: ["verified", "active", "retired"],
  failed: ["verified", "disabled", "retired"],
  retired: [],
};
export class ExtensionCatalog {
  private records = new Map<string, Entry>();
  private events: Array<{
    id: string;
    providerId: string;
    action: string;
    message: string;
    createdAt: string;
  }> = [];
  constructor(descriptors: ExtensionDescriptor[]) {
    for (const descriptor of descriptors)
      this.records.set(descriptor.providerId, {
        descriptor,
        lifecycleState: "active",
      });
  }
  list() {
    return [...this.records.values()];
  }
  get(id: string) {
    const value = this.records.get(id);
    if (!value)
      throw Object.assign(new Error(`Extension not found: ${id}`), {
        statusCode: 404,
      });
    return value;
  }
  require(id: string) {
    const value = this.get(id);
    if (!["active", "canary"].includes(value.lifecycleState))
      throw Object.assign(
        new Error(`Extension ${id} is ${value.lifecycleState}`),
        { statusCode: 409 },
      );
    return value;
  }
  probe(id: string) {
    const value = this.get(id);
    value.lastProbe = {
      status: ["active", "canary", "verified"].includes(value.lifecycleState)
        ? "ready"
        : "unavailable",
      checkedAt: new Date().toISOString(),
      reason: value.lifecycleState,
    };
    this.log(id, "probe", value.lastProbe.status);
    return value.lastProbe;
  }
  transition(id: string, state: ExtensionState) {
    const value = this.get(id);
    if (
      value.lifecycleState !== state &&
      !transitions[value.lifecycleState].includes(state)
    )
      throw Object.assign(
        new Error(
          `Cannot move extension from ${value.lifecycleState} to ${state}`,
        ),
        { statusCode: 409 },
      );
    value.lifecycleState = state;
    this.log(id, "lifecycle", `Extension moved to ${state}`);
    return value;
  }
  logs(id?: string) {
    return this.events
      .filter((event) => !id || event.providerId === id)
      .slice(-200)
      .reverse();
  }
  private log(providerId: string, action: string, message: string) {
    this.events.push({
      id: crypto.randomUUID(),
      providerId,
      action,
      message,
      createdAt: new Date().toISOString(),
    });
  }
}
export const resourceExtensions = new ExtensionCatalog([
  {
    providerId: "resource-backend.local-fs",
    family: "resource-backend",
    version: "1.0.0",
    contractVersion: "1.0",
    displayName: "Local Filesystem Backend",
    isolation: "process",
    capabilities: {
      objects: true,
      acl: true,
      retention: true,
      integrity: true,
    },
  },
  {
    providerId: "resource-processor.diagnostics-zip",
    family: "resource-processor",
    version: "1.0.0",
    contractVersion: "1.0",
    displayName: "Diagnostic Bundle Processor",
    isolation: "in-process",
    capabilities: { zip: true, redaction: true },
  },
  {
    providerId: "resource-processor.ffmpeg",
    family: "resource-processor",
    version: "1.0.0",
    contractVersion: "1.0",
    displayName: "FFmpeg Media Processor",
    isolation: "process",
    capabilities: { audio: true, video: true, gif: true, probe: true },
  },
]);

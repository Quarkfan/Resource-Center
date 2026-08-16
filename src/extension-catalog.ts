import { randomUUID } from "node:crypto";

export type ExtensionState =
  | "installed"
  | "verified"
  | "canary"
  | "active"
  | "draining"
  | "disabled"
  | "failed"
  | "retired";

export interface ExtensionDescriptor {
  providerId: string;
  family: string;
  version: string;
  contractVersion: string;
  displayName: string;
  isolation: "in-process" | "worker" | "process" | "container" | "remote";
  capabilities: Record<string, boolean | string | number>;
}

export interface ExtensionProbe {
  status: "ready" | "unavailable";
  checkedAt: string;
  reason?: string;
}

export interface ExtensionPersistedState {
  providerId: string;
  descriptorVersion: string;
  lifecycleState: ExtensionState;
  generation: number;
  installedAt: string;
  updatedAt: string;
  lastProbe?: ExtensionProbe;
}

export interface ExtensionRecord extends ExtensionPersistedState {
  descriptor: ExtensionDescriptor;
}

export interface ExtensionEvent {
  id: string;
  providerId: string;
  action: "install" | "upgrade" | "probe" | "lifecycle";
  message: string;
  createdAt: string;
}

export interface ExtensionStateRepository {
  extensionStates(): Promise<ExtensionPersistedState[]>;
  commitExtensionState(
    state: ExtensionPersistedState,
    event?: ExtensionEvent,
  ): Promise<void>;
  extensionEvents(
    providerId?: string,
    limit?: number,
  ): Promise<ExtensionEvent[]>;
  close?(): Promise<void>;
}

export class MemoryExtensionStateRepository implements ExtensionStateRepository {
  private readonly states = new Map<string, ExtensionPersistedState>();
  private readonly events: ExtensionEvent[] = [];

  async extensionStates() {
    return [...this.states.values()].map((value) => structuredClone(value));
  }

  async commitExtensionState(
    state: ExtensionPersistedState,
    event?: ExtensionEvent,
  ) {
    this.states.set(state.providerId, structuredClone(state));
    if (event) this.events.push(structuredClone(event));
  }

  async extensionEvents(providerId?: string, limit = 200) {
    return this.events
      .filter((event) => !providerId || event.providerId === providerId)
      .slice(-Math.max(1, Math.min(500, limit)))
      .reverse()
      .map((value) => structuredClone(value));
  }
}

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

const now = () => new Date().toISOString();

export class ExtensionCatalog {
  private readonly records = new Map<string, ExtensionRecord>();
  private readonly queues = new Map<string, Promise<void>>();
  private initialized = false;
  private initialization?: Promise<void>;

  constructor(
    descriptors: ExtensionDescriptor[],
    private readonly repository: ExtensionStateRepository = new MemoryExtensionStateRepository(),
  ) {
    const timestamp = now();
    for (const descriptor of descriptors) {
      this.records.set(descriptor.providerId, {
        descriptor,
        providerId: descriptor.providerId,
        descriptorVersion: descriptor.version,
        lifecycleState: "active",
        generation: 1,
        installedAt: timestamp,
        updatedAt: timestamp,
      });
    }
  }

  async initialize() {
    if (this.initialized) return;
    this.initialization ??= this.hydrate().catch((error: unknown) => {
      this.initialization = undefined;
      throw error;
    });
    await this.initialization;
  }

  private async hydrate() {
    const stored = new Map(
      (await this.repository.extensionStates()).map((value) => [
        value.providerId,
        value,
      ]),
    );
    for (const [providerId, candidate] of this.records) {
      const previous = stored.get(providerId);
      const upgraded =
        previous && previous.descriptorVersion !== candidate.descriptor.version;
      const state: ExtensionRecord = previous
        ? {
            ...candidate,
            ...previous,
            descriptor: candidate.descriptor,
            descriptorVersion: candidate.descriptor.version,
            generation: previous.generation + (upgraded ? 1 : 0),
            updatedAt: upgraded ? now() : previous.updatedAt,
          }
        : candidate;
      this.records.set(providerId, state);
      await this.repository.commitExtensionState(
        this.persisted(state),
        !previous
          ? this.event(providerId, "install", "Built-in extension installed")
          : upgraded
            ? this.event(
                providerId,
                "upgrade",
                `Extension upgraded from ${previous.descriptorVersion} to ${candidate.descriptor.version}`,
              )
            : undefined,
      );
    }
    this.initialized = true;
  }

  list() {
    return [...this.records.values()].map((value) => structuredClone(value));
  }

  get(id: string) {
    const record = this.records.get(id);
    if (!record)
      throw Object.assign(new Error(`Extension not found: ${id}`), {
        statusCode: 404,
      });
    return structuredClone(record);
  }

  require(id: string) {
    const record = this.records.get(id);
    if (!record)
      throw Object.assign(new Error(`Extension not found: ${id}`), {
        statusCode: 404,
      });
    if (!["active", "canary"].includes(record.lifecycleState))
      throw Object.assign(
        new Error(`Extension ${id} is ${record.lifecycleState}`),
        { statusCode: 409 },
      );
    return structuredClone(record);
  }

  async probe(id: string) {
    return this.exclusive(id, async () => this.performProbe(id));
  }

  async transition(id: string, state: ExtensionState) {
    return this.exclusive(id, async () => {
      const record = this.record(id);
      if (record.lifecycleState === state) return structuredClone(record);
      if (!transitions[record.lifecycleState].includes(state))
        throw Object.assign(
          new Error(
            `Cannot move extension from ${record.lifecycleState} to ${state}`,
          ),
          { statusCode: 409 },
        );
      if (["verified", "canary", "active"].includes(state)) {
        const probe = await this.performProbe(id);
        if (probe.status !== "ready")
          throw Object.assign(new Error(`Extension probe is ${probe.status}`), {
            statusCode: 409,
          });
      }
      record.lifecycleState = state;
      record.updatedAt = now();
      await this.repository.commitExtensionState(
        this.persisted(record),
        this.event(id, "lifecycle", `Extension moved to ${state}`),
      );
      return structuredClone(record);
    });
  }

  async logs(id?: string, limit = 200) {
    if (id) this.record(id);
    return this.repository.extensionEvents(id, limit);
  }

  async close() {
    await this.repository.close?.();
  }

  private async performProbe(id: string) {
    const record = this.record(id);
    const probe: ExtensionProbe = {
      status: record.lifecycleState === "retired" ? "unavailable" : "ready",
      checkedAt: now(),
      reason: record.lifecycleState,
    };
    record.lastProbe = probe;
    record.updatedAt = probe.checkedAt;
    await this.repository.commitExtensionState(
      this.persisted(record),
      this.event(id, "probe", probe.status),
    );
    return structuredClone(probe);
  }

  private record(id: string) {
    const record = this.records.get(id);
    if (!record)
      throw Object.assign(new Error(`Extension not found: ${id}`), {
        statusCode: 404,
      });
    return record;
  }

  private persisted(record: ExtensionRecord): ExtensionPersistedState {
    const { descriptor: _descriptor, ...state } = record;
    return structuredClone(state);
  }

  private event(
    providerId: string,
    action: ExtensionEvent["action"],
    message: string,
  ): ExtensionEvent {
    return { id: randomUUID(), providerId, action, message, createdAt: now() };
  }

  private async exclusive<T>(id: string, operation: () => Promise<T>) {
    const previous = this.queues.get(id) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const queued = previous.then(() => current);
    this.queues.set(id, queued);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.queues.get(id) === queued) this.queues.delete(id);
    }
  }
}

import type { CleanupPlan, MediaJob, ResourceItem } from "./types.js";
export interface ResourceRepository {
  migrate(): Promise<void>;
  ping(): Promise<boolean>;
  close(): Promise<void>;
  save(v: ResourceItem): Promise<ResourceItem>;
  get(id: string): Promise<ResourceItem | undefined>;
  byHash(
    tenantId: string,
    hash: string,
    kind: string,
  ): Promise<ResourceItem | undefined>;
  list(tenantId?: string): Promise<ResourceItem[]>;
  remove(id: string): Promise<boolean>;
  savePlan(v: CleanupPlan): Promise<CleanupPlan>;
  plan(id: string): Promise<CleanupPlan | undefined>;
  saveJob(v: MediaJob): Promise<MediaJob>;
  job(id: string): Promise<MediaJob | undefined>;
  jobs(tenantId?: string): Promise<MediaJob[]>;
}
export class MemoryResourceRepository implements ResourceRepository {
  r = new Map<string, ResourceItem>();
  p = new Map<string, CleanupPlan>();
  j = new Map<string, MediaJob>();
  async migrate() {}
  async ping() {
    return true;
  }
  async close() {}
  async save(v: ResourceItem) {
    this.r.set(v.id, structuredClone(v));
    return v;
  }
  async get(id: string) {
    return this.r.get(id);
  }
  async byHash(t: string, h: string, k: string) {
    return [...this.r.values()].find(
      (x) => x.tenantId === t && x.sha256 === h && x.kind === k,
    );
  }
  async list(t?: string) {
    return [...this.r.values()].filter((x) => !t || x.tenantId === t);
  }
  async remove(id: string) {
    return this.r.delete(id);
  }
  async savePlan(v: CleanupPlan) {
    this.p.set(v.id, structuredClone(v));
    return v;
  }
  async plan(id: string) {
    return this.p.get(id);
  }
  async saveJob(v: MediaJob) {
    this.j.set(v.id, structuredClone(v));
    return v;
  }
  async job(id: string) {
    const value = this.j.get(id);
    return value ? structuredClone(value) : undefined;
  }
  async jobs(t?: string) {
    return [...this.j.values()]
      .filter((x) => !t || x.tenantId === t)
      .map((x) => structuredClone(x));
  }
}

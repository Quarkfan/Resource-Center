import { Pool } from "pg";
import type { ResourceRepository } from "./repository.js";
import type { CleanupPlan, MediaJob, ResourceItem } from "./types.js";
const schema = `CREATE SCHEMA IF NOT EXISTS res;CREATE TABLE IF NOT EXISTS res.items(id uuid PRIMARY KEY,tenant_id text NOT NULL,sha256 text NOT NULL,kind text NOT NULL,data jsonb NOT NULL,created_at timestamptz NOT NULL,UNIQUE(tenant_id,sha256,kind));CREATE INDEX IF NOT EXISTS items_tenant_idx ON res.items(tenant_id,kind);CREATE TABLE IF NOT EXISTS res.cleanup_plans(id uuid PRIMARY KEY,tenant_id text NOT NULL,data jsonb NOT NULL,created_at timestamptz NOT NULL);CREATE TABLE IF NOT EXISTS res.media_jobs(id uuid PRIMARY KEY,tenant_id text NOT NULL,status text NOT NULL,data jsonb NOT NULL,created_at timestamptz NOT NULL);`;
export class PgResourceRepository implements ResourceRepository {
  private p: Pool;
  constructor(url: string) {
    this.p = new Pool({ connectionString: url, max: 10 });
  }
  async migrate() {
    await this.p.query(schema);
  }
  async ping() {
    try {
      await this.p.query("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }
  async close() {
    await this.p.end();
  }
  async save(v: ResourceItem) {
    await this.p.query(
      "INSERT INTO res.items(id,tenant_id,sha256,kind,data,created_at)VALUES($1,$2,$3,$4,$5,$6)ON CONFLICT(id)DO UPDATE SET data=$5",
      [v.id, v.tenantId, v.sha256, v.kind, v, v.createdAt],
    );
    return v;
  }
  async get(id: string) {
    return (await this.p.query("SELECT data FROM res.items WHERE id=$1", [id]))
      .rows[0]?.data;
  }
  async byHash(t: string, h: string, k: string) {
    return (
      await this.p.query(
        "SELECT data FROM res.items WHERE tenant_id=$1 AND sha256=$2 AND kind=$3",
        [t, h, k],
      )
    ).rows[0]?.data;
  }
  async list(t?: string) {
    return (
      await this.p.query(
        `SELECT data FROM res.items ${t ? "WHERE tenant_id=$1" : ""} ORDER BY created_at DESC`,
        t ? [t] : [],
      )
    ).rows.map((x) => x.data);
  }
  async remove(id: string) {
    return (
      (await this.p.query("DELETE FROM res.items WHERE id=$1", [id]))
        .rowCount === 1
    );
  }
  async savePlan(v: CleanupPlan) {
    await this.p.query(
      "INSERT INTO res.cleanup_plans(id,tenant_id,data,created_at)VALUES($1,$2,$3,$4)ON CONFLICT(id)DO UPDATE SET data=$3",
      [v.id, v.tenantId, v, v.createdAt],
    );
    return v;
  }
  async plan(id: string) {
    return (
      await this.p.query("SELECT data FROM res.cleanup_plans WHERE id=$1", [id])
    ).rows[0]?.data;
  }
  async saveJob(v: MediaJob) {
    await this.p.query(
      "INSERT INTO res.media_jobs(id,tenant_id,status,data,created_at)VALUES($1,$2,$3,$4,$5)ON CONFLICT(id)DO UPDATE SET status=$3,data=$4",
      [v.id, v.tenantId, v.status, v, v.createdAt],
    );
    return v;
  }
  async job(id: string) {
    return (
      await this.p.query("SELECT data FROM res.media_jobs WHERE id=$1", [id])
    ).rows[0]?.data;
  }
  async jobs(t?: string) {
    return (
      await this.p.query(
        `SELECT data FROM res.media_jobs ${t ? "WHERE tenant_id=$1" : ""} ORDER BY created_at DESC`,
        t ? [t] : [],
      )
    ).rows.map((x) => x.data);
  }
}

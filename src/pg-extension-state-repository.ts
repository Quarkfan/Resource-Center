import { Pool } from "pg";
import type {
  ExtensionEvent,
  ExtensionPersistedState,
  ExtensionStateRepository,
} from "./extension-catalog.js";

export class PgExtensionStateRepository implements ExtensionStateRepository {
  private readonly pool: Pool;
  private readonly statesTable: string;
  private readonly eventsTable: string;

  constructor(databaseUrl: string, schema: string) {
    if (!/^[a-z][a-z0-9_]*$/.test(schema))
      throw new Error("Invalid extension state schema");
    this.pool = new Pool({ connectionString: databaseUrl, max: 2 });
    this.statesTable = `${schema}.extension_states`;
    this.eventsTable = `${schema}.extension_events`;
  }

  async migrate() {
    const schema = this.statesTable.split(".")[0];
    await this.pool.query(`
      CREATE SCHEMA IF NOT EXISTS ${schema};
      CREATE TABLE IF NOT EXISTS ${this.statesTable} (
        provider_id text PRIMARY KEY,
        data jsonb NOT NULL,
        updated_at timestamptz NOT NULL
      );
      CREATE TABLE IF NOT EXISTS ${this.eventsTable} (
        id uuid PRIMARY KEY,
        provider_id text NOT NULL,
        data jsonb NOT NULL,
        created_at timestamptz NOT NULL
      );
      CREATE INDEX IF NOT EXISTS extension_events_provider_created_idx
        ON ${this.eventsTable}(provider_id, created_at DESC);
    `);
  }

  async extensionStates(): Promise<ExtensionPersistedState[]> {
    return (
      await this.pool.query(`SELECT data FROM ${this.statesTable}`)
    ).rows.map((row) => row.data);
  }

  async commitExtensionState(
    state: ExtensionPersistedState,
    event?: ExtensionEvent,
  ) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO ${this.statesTable}(provider_id,data,updated_at)
         VALUES($1,$2,$3)
         ON CONFLICT(provider_id) DO UPDATE
         SET data=EXCLUDED.data,updated_at=EXCLUDED.updated_at`,
        [state.providerId, state, state.updatedAt],
      );
      if (event)
        await client.query(
          `INSERT INTO ${this.eventsTable}(id,provider_id,data,created_at)
           VALUES($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING`,
          [event.id, event.providerId, event, event.createdAt],
        );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async extensionEvents(providerId?: string, limit = 200) {
    const bounded = Math.max(1, Math.min(500, limit));
    return (
      await this.pool.query(
        `SELECT data FROM ${this.eventsTable}
         ${providerId ? "WHERE provider_id=$1" : ""}
         ORDER BY created_at DESC LIMIT $${providerId ? 2 : 1}`,
        providerId ? [providerId, bounded] : [bounded],
      )
    ).rows.map((row) => row.data as ExtensionEvent);
  }

  async close() {
    await this.pool.end();
  }
}

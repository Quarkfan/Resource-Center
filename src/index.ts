import { buildApp } from "./app.js";
import { MemoryResourceRepository } from "./repository.js";
import { PgResourceRepository } from "./pg-repository.js";
import { requireInternalServiceToken } from "./config.js";
import { PgExtensionStateRepository } from "./pg-extension-state-repository.js";
const repo = process.env.DATABASE_URL
  ? new PgResourceRepository(process.env.DATABASE_URL)
  : new MemoryResourceRepository();
await repo.migrate();
const extensionRepository = process.env.DATABASE_URL
  ? new PgExtensionStateRepository(process.env.DATABASE_URL, "res")
  : undefined;
await extensionRepository?.migrate();
await buildApp({
  repository: repo,
  extensionRepository,
  internalToken: requireInternalServiceToken(),
  root: process.env.RESOURCE_ROOT ?? "./data",
  ffmpeg: process.env.FFMPEG_PATH ?? "ffmpeg",
  ffprobe: process.env.FFPROBE_PATH ?? "ffprobe",
  mediaConcurrency: Number(process.env.MEDIA_CONCURRENCY ?? 1),
}).listen({
  host: process.env.HOST ?? "0.0.0.0",
  port: Number(process.env.PORT ?? 4107),
});

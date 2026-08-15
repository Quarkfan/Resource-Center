import { buildApp } from "./app.js";
import { MemoryResourceRepository } from "./repository.js";
import { PgResourceRepository } from "./pg-repository.js";
const repo = process.env.DATABASE_URL
  ? new PgResourceRepository(process.env.DATABASE_URL)
  : new MemoryResourceRepository();
await repo.migrate();
await buildApp({
  repository: repo,
  internalToken: process.env.INTERNAL_SERVICE_TOKEN ?? "development-only",
  root: process.env.RESOURCE_ROOT ?? "./data",
  ffmpeg: process.env.FFMPEG_PATH ?? "ffmpeg",
  ffprobe: process.env.FFPROBE_PATH ?? "ffprobe",
  mediaConcurrency: Number(process.env.MEDIA_CONCURRENCY ?? 1),
}).listen({
  host: process.env.HOST ?? "0.0.0.0",
  port: Number(process.env.PORT ?? 4107),
});

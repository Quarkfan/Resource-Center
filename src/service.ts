import { createHash, randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { spawn } from "node:child_process";
import archiver from "archiver";
import type { ResourceRepository } from "./repository.js";
import type { MediaJob, ResourceItem, ResourceKind } from "./types.js";
const now = () => new Date().toISOString();
export interface ProcessRunner {
  run(
    command: string,
    args: string[],
    options?: {
      signal?: AbortSignal;
      onProgress?: (progress: number) => void;
    },
  ): Promise<void | { stdout?: string; stderr?: string }>;
}
export class SpawnRunner implements ProcessRunner {
  run(
    c: string,
    a: string[],
    options?: {
      signal?: AbortSignal;
      onProgress?: (progress: number) => void;
    },
  ) {
    return new Promise<{ stdout: string; stderr: string }>(
      (resolve, reject) => {
        const p = spawn(c, a, {
          shell: false,
          stdio: ["ignore", "pipe", "pipe"],
        });
        let e = "",
          o = "",
          settled = false,
          progress = 15;
        const abort = () => {
          if (p.exitCode !== null) return;
          p.kill("SIGTERM");
          const timer = setTimeout(() => p.kill("SIGKILL"), 5_000);
          timer.unref();
        };
        if (options?.signal?.aborted) abort();
        options?.signal?.addEventListener("abort", abort, { once: true });
        p.stdout.on("data", (x) => (o += x.toString()));
        p.stderr.on("data", (x) => {
          e += x.toString();
          progress = Math.min(90, progress + 2);
          options?.onProgress?.(progress);
        });
        p.on("error", (error) => {
          if (settled) return;
          settled = true;
          options?.signal?.removeEventListener("abort", abort);
          reject(error);
        });
        p.on("close", (code) => {
          if (settled) return;
          settled = true;
          options?.signal?.removeEventListener("abort", abort);
          if (options?.signal?.aborted)
            reject(
              Object.assign(new Error("Media job cancelled"), {
                name: "AbortError",
              }),
            );
          else if (code === 0) resolve({ stdout: o, stderr: e });
          else reject(new Error(e.slice(-4000) || `process exited ${code}`));
        });
      },
    );
  }
}
export interface MediaRequest {
  tenantId: string;
  botId?: string;
  operation: string;
  inputIds: string[];
  outputName: string;
  mediaType: string;
  params: Record<string, unknown>;
}
export class ResourceService {
  private readonly active = new Map<string, AbortController>();
  private readonly workers = new Set<Promise<void>>();
  private draining = false;
  private stopping = false;

  constructor(
    readonly repo: ResourceRepository,
    readonly root: string,
    readonly ffmpeg: string,
    readonly runner: ProcessRunner = new SpawnRunner(),
    readonly ffprobe = "ffprobe",
    readonly mediaConcurrency = 1,
  ) {}
  async init() {
    await mkdir(join(this.root, "objects"), { recursive: true });
    await mkdir(join(this.root, "tmp"), { recursive: true });
    await mkdir(join(this.root, "diagnostics"), { recursive: true });
    for (const job of await this.repo.jobs()) {
      job.progress ??= job.status === "succeeded" ? 100 : 0;
      job.attempts ??= job.startedAt ? 1 : 0;
      job.recoveryCount ??= 0;
      job.cancelRequested ??= false;
      job.updatedAt ??= job.finishedAt ?? job.startedAt ?? job.createdAt;
      if (
        (job.status === "running" || job.status === "queued") &&
        !job.request
      ) {
        job.status = "failed";
        job.error = "Legacy media job cannot be recovered; submit it again";
        job.finishedAt = now();
        job.updatedAt = job.finishedAt;
      } else if (job.status === "running") {
        job.status = "queued";
        job.progress = 0;
        job.cancelRequested = false;
        job.recoveryCount += 1;
        job.error = "Recovered after service restart";
        job.updatedAt = now();
        delete job.startedAt;
        delete job.finishedAt;
      }
      await this.repo.saveJob(job);
    }
    this.scheduleDrain();
  }
  async shutdown() {
    this.stopping = true;
    for (const controller of this.active.values()) controller.abort();
    await Promise.allSettled([...this.workers]);
  }
  async put(i: {
    tenantId: string;
    botId?: string;
    kind: ResourceKind;
    name: string;
    mediaType: string;
    data: Buffer;
    metadata?: Record<string, unknown>;
    expiresAt?: string;
  }) {
    const sha256 = createHash("sha256").update(i.data).digest("hex"),
      old = await this.repo.byHash(i.tenantId, sha256, i.kind);
    if (old) {
      old.lastAccessedAt = now();
      old.refs++;
      old.allowedBotIds ??= old.botId ? [old.botId] : [];
      if (i.botId && !old.allowedBotIds.includes(i.botId))
        old.allowedBotIds.push(i.botId);
      await this.repo.save(old);
      return { item: old, created: false };
    }
    const dir = join(this.root, "objects", sha256.slice(0, 2));
    await mkdir(dir, { recursive: true });
    const path = join(dir, sha256);
    try {
      await stat(path);
    } catch {
      const tmp = join(this.root, "tmp", randomUUID());
      await writeFile(tmp, i.data, { mode: 0o600 });
      await rename(tmp, path);
    }
    const n = now(),
      item: ResourceItem = {
        id: randomUUID(),
        tenantId: i.tenantId,
        botId: i.botId,
        allowedBotIds: i.botId ? [i.botId] : [],
        kind: i.kind,
        sha256,
        size: i.data.length,
        mediaType: i.mediaType,
        name: i.name,
        path,
        refs: 1,
        metadata: i.metadata ?? {},
        createdAt: n,
        lastAccessedAt: n,
        expiresAt: i.expiresAt,
      };
    await this.repo.save(item);
    return { item, created: true };
  }
  async read(id: string, tenantId: string, botId?: string) {
    const v = await this.repo.get(id);
    if (!v || v.tenantId !== tenantId)
      throw Object.assign(new Error("Resource not found"), { statusCode: 404 });
    v.allowedBotIds ??= v.botId ? [v.botId] : [];
    if (botId && v.allowedBotIds.length && !v.allowedBotIds.includes(botId))
      throw Object.assign(new Error("Resource not found"), { statusCode: 404 });
    v.lastAccessedAt = now();
    await this.repo.save(v);
    return { item: v, data: await readFile(v.path) };
  }
  async list(tenantId?: string, botId?: string) {
    const items = await this.repo.list(tenantId);
    return items.filter((item) => {
      item.allowedBotIds ??= item.botId ? [item.botId] : [];
      return (
        !botId ||
        !item.allowedBotIds.length ||
        item.allowedBotIds.includes(botId)
      );
    });
  }
  async setAcl(id: string, tenantId: string, allowedBotIds: string[]) {
    const item = await this.requireItem(id, tenantId);
    item.allowedBotIds = [...new Set(allowedBotIds)].sort();
    if (item.botId && !item.allowedBotIds.includes(item.botId))
      item.allowedBotIds.unshift(item.botId);
    await this.repo.save(item);
    return item;
  }
  async retain(id: string, tenantId: string, delta: 1 | -1) {
    const item = await this.requireItem(id, tenantId);
    item.refs = Math.max(0, item.refs + delta);
    item.lastAccessedAt = now();
    return this.repo.save(item);
  }
  async stats(tenantId: string) {
    const items = await this.repo.list(tenantId),
      physical = new Map<string, number>(),
      byKind: Record<string, { count: number; bytes: number }> = {},
      byBot: Record<string, { count: number; bytes: number }> = {};
    for (const item of items) {
      physical.set(item.sha256, item.size);
      const kind = (byKind[item.kind] ??= { count: 0, bytes: 0 });
      kind.count++;
      kind.bytes += item.size;
      for (const botId of item.allowedBotIds ??
        (item.botId ? [item.botId] : [])) {
        const bot = (byBot[botId] ??= { count: 0, bytes: 0 });
        bot.count++;
        bot.bytes += item.size;
      }
    }
    const logicalBytes = items.reduce((sum, item) => sum + item.size, 0);
    return {
      tenantId,
      count: items.length,
      logicalBytes,
      physicalBytes: [...physical.values()].reduce(
        (sum, size) => sum + size,
        0,
      ),
      deduplicatedBytes:
        logicalBytes -
        [...physical.values()].reduce((sum, size) => sum + size, 0),
      referenced: items.filter((item) => item.refs > 0).length,
      unreferenced: items.filter((item) => item.refs <= 0).length,
      byKind,
      byBot,
    };
  }
  async checkIntegrity(input: {
    tenantId: string;
    dryRun?: boolean;
    removeMissing?: boolean;
  }) {
    const results: Array<{
      id: string;
      status: "healthy" | "missing" | "corrupt";
      expectedSha256: string;
      actualSha256?: string;
      repaired: boolean;
    }> = [];
    for (const item of await this.repo.list(input.tenantId)) {
      let status: "healthy" | "missing" | "corrupt" = "healthy";
      let actualSha256: string | undefined;
      try {
        const data = await readFile(item.path);
        actualSha256 = createHash("sha256").update(data).digest("hex");
        if (actualSha256 !== item.sha256 || data.length !== item.size)
          status = "corrupt";
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT")
          status = "missing";
        else throw error;
      }
      let repaired = false;
      if (!input.dryRun && status === "missing" && input.removeMissing) {
        await this.repo.remove(item.id);
        repaired = true;
      } else if (!input.dryRun && status === "corrupt") {
        item.metadata = {
          ...item.metadata,
          integrity: { status, checkedAt: now(), actualSha256 },
        };
        await this.repo.save(item);
        repaired = true;
      }
      results.push({
        id: item.id,
        status,
        expectedSha256: item.sha256,
        actualSha256,
        repaired,
      });
    }
    return {
      tenantId: input.tenantId,
      dryRun: input.dryRun ?? true,
      checked: results.length,
      healthy: results.filter((item) => item.status === "healthy").length,
      missing: results.filter((item) => item.status === "missing").length,
      corrupt: results.filter((item) => item.status === "corrupt").length,
      results,
      checkedAt: now(),
    };
  }
  private async requireItem(id: string, tenantId: string) {
    const item = await this.repo.get(id);
    if (!item || item.tenantId !== tenantId)
      throw Object.assign(new Error("Resource not found"), { statusCode: 404 });
    item.allowedBotIds ??= item.botId ? [item.botId] : [];
    return item;
  }
  async planCleanup(i: {
    tenantId: string;
    olderThan: string;
    kinds?: ResourceKind[];
    dryRun?: boolean;
  }) {
    const candidates = (await this.repo.list(i.tenantId))
        .filter(
          (x) =>
            (!i.kinds?.length || i.kinds.includes(x.kind)) &&
            x.lastAccessedAt < i.olderThan &&
            x.refs <= 0,
        )
        .map((x) => ({
          id: x.id,
          size: x.size,
          reason: "expired or unreferenced",
        })),
      v = {
        id: randomUUID(),
        tenantId: i.tenantId,
        dryRun: i.dryRun ?? true,
        candidates,
        totalBytes: candidates.reduce((n, x) => n + x.size, 0),
        createdAt: now(),
      };
    return this.repo.savePlan(v);
  }
  async executeCleanup(id: string) {
    const p = await this.repo.plan(id);
    if (!p)
      throw Object.assign(new Error("Cleanup plan not found"), {
        statusCode: 404,
      });
    if (!p.dryRun)
      for (const c of p.candidates) {
        const v = await this.repo.get(c.id);
        if (v && v.refs <= 0) {
          await rm(v.path, { force: true });
          await this.repo.remove(v.id);
        }
      }
    p.executedAt = now();
    return this.repo.savePlan(p);
  }
  async diagnostics(i: {
    tenantId: string;
    sections: Record<string, unknown>;
    logs?: Array<{ name: string; content: string }>;
  }) {
    const redactText = (value: string) =>
      value
        .replace(/Bearer\s+[A-Za-z0-9._~-]+/gi, "Bearer [REDACTED]")
        .replace(
          /((?:api[-_]?key|token|secret|password|passwd|cookie|credential)\s*["']?\s*[:=]\s*["']?)[^"',;\s}]+/gi,
          "$1[REDACTED]",
        )
        .replace(/\bsk-[A-Za-z0-9_-]{16,}\b/g, "[REDACTED]");
    const safe = (v: unknown): unknown =>
      Array.isArray(v)
        ? v.map(safe)
        : v && typeof v === "object"
          ? Object.fromEntries(
              Object.entries(v).map(([k, x]) => [
                /token|secret|password|authorization|cookie|key/i.test(k)
                  ? k
                  : k,
                /token|secret|password|authorization|cookie|key/i.test(k)
                  ? "[REDACTED]"
                  : safe(x),
              ]),
            )
          : typeof v === "string"
            ? redactText(v)
            : v;
    const out = join(this.root, "diagnostics", `${randomUUID()}.zip`);
    await new Promise<void>((resolve, reject) => {
      const output = createWriteStream(out, { mode: 0o600 }),
        zip = archiver("zip", { zlib: { level: 9 } });
      output.on("close", resolve);
      zip.on("error", reject);
      zip.pipe(output);
      zip.append(JSON.stringify(safe(i.sections), null, 2), {
        name: "system.json",
      });
      for (const l of i.logs ?? [])
        zip.append(redactText(l.content), {
          name: `logs/${l.name.replace(/[^a-zA-Z0-9._-]/g, "_")}`,
        });
      void zip.finalize();
    });
    const data = await readFile(out);
    await rm(out, { force: true });
    return this.put({
      tenantId: i.tenantId,
      kind: "diagnostic",
      name: "quarkfan-diagnostics.zip",
      mediaType: "application/zip",
      data,
      metadata: { sanitized: true },
    });
  }
  mediaArgs(
    op: string,
    inputs: string[],
    output: string,
    p: Record<string, unknown>,
  ) {
    const first = inputs[0];
    if (!first)
      throw Object.assign(new Error("At least one input is required"), {
        statusCode: 400,
      });
    switch (op) {
      case "export":
      case "transcode":
        return [
          "-y",
          "-i",
          first,
          "-c:v",
          String(p.videoCodec ?? "libx264"),
          "-c:a",
          String(p.audioCodec ?? "aac"),
          output,
        ];
      case "thumbnail":
        return [
          "-y",
          "-ss",
          String(p.at ?? "00:00:01"),
          "-i",
          first,
          "-frames:v",
          "1",
          output,
        ];
      case "extract-audio":
        return [
          "-y",
          "-i",
          first,
          "-vn",
          "-c:a",
          String(p.codec ?? "libmp3lame"),
          output,
        ];
      case "trim":
        return [
          "-y",
          "-ss",
          String(p.start ?? "0"),
          "-i",
          first,
          "-t",
          String(p.duration ?? "10"),
          "-c",
          "copy",
          output,
        ];
      case "scale":
      case "resize":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `scale=${Number(p.width ?? 1280)}:${Number(p.height ?? -2)}`,
          output,
        ];
      case "merge": {
        if (inputs.length < 2)
          throw Object.assign(new Error("Merge requires at least two inputs"), {
            statusCode: 400,
          });
        return [
          "-y",
          ...inputs.flatMap((input) => ["-i", input]),
          "-filter_complex",
          `concat=n=${inputs.length}:v=1:a=1`,
          output,
        ];
      }
      case "crop":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `crop=${positive(p.width, 640)}:${positive(p.height, 360)}:${nonnegative(p.x, 0)}:${nonnegative(p.y, 0)}`,
          output,
        ];
      case "rotate": {
        const degrees = Number(p.degrees ?? 90);
        const filter =
          degrees === 90
            ? "transpose=1"
            : degrees === -90 || degrees === 270
              ? "transpose=2"
              : degrees === 180
                ? "hflip,vflip"
                : undefined;
        if (!filter)
          throw Object.assign(
            new Error("Rotate supports 90, -90, 180 or 270 degrees"),
            {
              statusCode: 400,
            },
          );
        return ["-y", "-i", first, "-vf", filter, output];
      }
      case "color":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `eq=brightness=${range(p.brightness, -1, 1, 0)}:contrast=${range(p.contrast, 0, 2, 1)}:saturation=${range(p.saturation, 0, 3, 1)}`,
          output,
        ];
      case "audio":
        return [
          "-y",
          "-i",
          first,
          "-af",
          `volume=${range(p.volume, 0, 10, 1)}`,
          output,
        ];
      case "text":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `drawtext=text='${filterText(String(p.text ?? ""))}':x=${position(p.x, "(w-text_w)/2")}:y=${position(p.y, "h-text_h-24")}:fontsize=${positive(p.fontSize, 32)}:fontcolor=${color(p.color, "white")}`,
          output,
        ];
      case "subtitle": {
        const subtitle = inputs[1];
        if (!subtitle)
          throw Object.assign(new Error("Subtitle requires a second input"), {
            statusCode: 400,
          });
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `subtitles='${filterPath(subtitle)}'`,
          output,
        ];
      }
      case "speed": {
        const speed = range(p.speed, 0.5, 2, 1);
        return [
          "-y",
          "-i",
          first,
          "-filter_complex",
          `[0:v]setpts=${1 / speed}*PTS[v];[0:a]atempo=${speed}[a]`,
          "-map",
          "[v]",
          "-map",
          "[a]",
          output,
        ];
      }
      case "compress":
        return [
          "-y",
          "-i",
          first,
          "-c:v",
          "libx264",
          "-crf",
          String(range(p.crf, 0, 51, 28)),
          "-preset",
          preset(p.preset),
          "-c:a",
          "aac",
          "-b:a",
          `${positive(p.audioKbps, 128)}k`,
          output,
        ];
      case "normalize":
        return [
          "-y",
          "-i",
          first,
          "-af",
          "loudnorm=I=-16:LRA=11:TP=-1.5",
          output,
        ];
      case "fade":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `fade=t=${p.direction === "out" ? "out" : "in"}:st=${nonnegative(p.start, 0)}:d=${positive(p.duration, 1)}`,
          output,
        ];
      case "watermark": {
        const watermark = inputs[1];
        if (!watermark)
          throw Object.assign(new Error("Watermark requires a second input"), {
            statusCode: 400,
          });
        return [
          "-y",
          "-i",
          first,
          "-i",
          watermark,
          "-filter_complex",
          `overlay=${position(p.x, "W-w-16")}:${position(p.y, "H-h-16")}`,
          output,
        ];
      }
      case "blur":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `boxblur=${positive(p.radius, 8)}:${positive(p.power, 1)}`,
          output,
        ];
      case "gif":
        return [
          "-y",
          "-i",
          first,
          "-vf",
          `fps=${Number(p.fps ?? 10)},scale=${Number(p.width ?? 640)}:-1:flags=lanczos`,
          output,
        ];
      default:
        throw Object.assign(new Error("Unsupported media operation"), {
          statusCode: 400,
        });
    }
  }
  async enqueueMedia(i: MediaRequest) {
    const resources = await this.resolveInputs(i.tenantId, i.inputIds);
    const extension = extname(i.outputName).toLowerCase();
    const output = join(
      this.root,
      "tmp",
      `${randomUUID()}${/^\.[a-z0-9]{1,10}$/.test(extension) ? extension : ""}`,
    );
    const args =
      i.operation === "info"
        ? ["-show_format", "-show_streams", "[RESOURCE]"]
        : this.mediaArgs(
            i.operation,
            resources.map((x) => x.path),
            output,
            i.params,
          ).map((value) =>
            resources.some((resource) => resource.path === value)
              ? "[RESOURCE]"
              : value === output
                ? "[OUTPUT]"
                : value,
          );
    const timestamp = now();
    const job: MediaJob = {
      id: randomUUID(),
      tenantId: i.tenantId,
      botId: i.botId,
      operation: i.operation,
      inputIds: [...i.inputIds],
      args,
      status: "queued",
      progress: 0,
      attempts: 0,
      recoveryCount: 0,
      cancelRequested: false,
      request: {
        outputName: i.outputName,
        mediaType: i.mediaType,
        params: structuredClone(i.params),
      },
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    await this.repo.saveJob(job);
    this.scheduleDrain();
    return job;
  }

  async getMediaJob(id: string, tenantId?: string) {
    const job = await this.repo.job(id);
    if (!job || (tenantId && job.tenantId !== tenantId))
      throw Object.assign(new Error("Media job not found"), {
        statusCode: 404,
      });
    return job;
  }

  async cancelMediaJob(id: string, tenantId?: string) {
    const job = await this.getMediaJob(id, tenantId);
    if (["succeeded", "failed", "cancelled"].includes(job.status)) return job;
    job.cancelRequested = true;
    job.updatedAt = now();
    if (job.status === "queued") {
      job.status = "cancelled";
      job.finishedAt = job.updatedAt;
    }
    await this.repo.saveJob(job);
    this.active.get(id)?.abort();
    return job;
  }

  async retryMediaJob(id: string, tenantId?: string) {
    const job = await this.getMediaJob(id, tenantId);
    if (!job.request)
      throw Object.assign(
        new Error("Legacy media job cannot be retried; submit it again"),
        {
          statusCode: 409,
        },
      );
    if (!["failed", "cancelled"].includes(job.status))
      throw Object.assign(
        new Error("Only failed or cancelled jobs can be retried"),
        {
          statusCode: 409,
        },
      );
    job.status = "queued";
    job.progress = 0;
    job.cancelRequested = false;
    job.error = undefined;
    job.result = undefined;
    job.outputId = undefined;
    job.startedAt = undefined;
    job.finishedAt = undefined;
    job.updatedAt = now();
    await this.repo.saveJob(job);
    this.scheduleDrain();
    return job;
  }

  async waitForMediaJob(id: string, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const job = await this.getMediaJob(id);
      if (["succeeded", "failed", "cancelled"].includes(job.status)) return job;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw Object.assign(new Error("Timed out waiting for media job"), {
      statusCode: 504,
    });
  }

  async runMedia(i: MediaRequest) {
    const queued = await this.enqueueMedia(i);
    const job = await this.waitForMediaJob(queued.id);
    if (job.status !== "succeeded")
      throw new Error(job.error ?? `Media job ${job.status}`);
    return {
      job,
      output: job.outputId ? await this.repo.get(job.outputId) : undefined,
    };
  }

  private scheduleDrain() {
    if (this.draining || this.stopping) return;
    this.draining = true;
    queueMicrotask(() => void this.drain());
  }

  private async drain() {
    try {
      while (
        !this.stopping &&
        this.workers.size < Math.max(1, this.mediaConcurrency)
      ) {
        const next = (await this.repo.jobs()).find(
          (job) =>
            job.status === "queued" &&
            !job.cancelRequested &&
            !this.active.has(job.id),
        );
        if (!next) break;
        const worker = this.executeMediaJob(next.id).finally(() => {
          this.workers.delete(worker);
          this.scheduleDrain();
        });
        this.workers.add(worker);
      }
    } finally {
      this.draining = false;
    }
  }

  private async executeMediaJob(id: string) {
    const job = await this.getMediaJob(id);
    if (job.status !== "queued" || job.cancelRequested) return;
    const controller = new AbortController();
    this.active.set(id, controller);
    job.status = "running";
    job.progress = 5;
    job.attempts = (job.attempts ?? 0) + 1;
    job.startedAt = now();
    job.updatedAt = job.startedAt;
    job.error = undefined;
    await this.repo.saveJob(job);
    let output: string | undefined;
    try {
      const resources = await this.resolveInputs(job.tenantId, job.inputIds);
      if (job.operation === "info") {
        const result = await this.runner.run(
          this.ffprobe,
          [
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-of",
            "json",
            resources[0]!.path,
          ],
          { signal: controller.signal },
        );
        job.result = JSON.parse(
          result && typeof result === "object" ? (result.stdout ?? "{}") : "{}",
        ) as Record<string, unknown>;
      } else {
        const extension = extname(job.request.outputName).toLowerCase();
        output = join(
          this.root,
          "tmp",
          `${job.id}${/^\.[a-z0-9]{1,10}$/.test(extension) ? extension : ""}`,
        );
        const args = this.mediaArgs(
          job.operation,
          resources.map((resource) => resource.path),
          output,
          job.request.params,
        );
        let lastSavedProgress = job.progress;
        let progressWrite = Promise.resolve();
        await this.runner.run(this.ffmpeg, args, {
          signal: controller.signal,
          onProgress: (progress) => {
            if (progress <= lastSavedProgress) return;
            lastSavedProgress = progress;
            progressWrite = progressWrite.then(async () => {
              const current = await this.repo.job(job.id);
              if (
                !current ||
                current.status !== "running" ||
                progress <= current.progress
              )
                return;
              current.progress = progress;
              current.updatedAt = now();
              await this.repo.saveJob(current);
            });
          },
        });
        await progressWrite;
        if (controller.signal.aborted)
          throw Object.assign(new Error("Media job cancelled"), {
            name: "AbortError",
          });
        const data = await readFile(output);
        const saved = await this.put({
          tenantId: job.tenantId,
          botId: job.botId,
          kind: "artifact",
          name: job.request.outputName,
          mediaType: job.request.mediaType,
          data,
          metadata: { mediaJobId: job.id, operation: job.operation },
        });
        job.outputId = saved.item.id;
      }
      job.status = "succeeded";
      job.progress = 100;
    } catch (error) {
      const cancelled =
        controller.signal.aborted ||
        (error instanceof Error && error.name === "AbortError");
      job.status = cancelled ? "cancelled" : "failed";
      job.error = cancelled
        ? "Media job cancelled"
        : error instanceof Error
          ? error.message
          : "media failed";
    } finally {
      if (output) await rm(output, { force: true });
      job.finishedAt = now();
      job.updatedAt = job.finishedAt;
      await this.repo.saveJob(job);
      this.active.delete(id);
    }
  }

  private async resolveInputs(tenantId: string, inputIds: string[]) {
    const resources: ResourceItem[] = [];
    for (const id of inputIds) {
      const resource = await this.repo.get(id);
      if (!resource || resource.tenantId !== tenantId)
        throw Object.assign(new Error("Resource not found"), {
          statusCode: 404,
        });
      resources.push(resource);
    }
    return resources;
  }
}

function finite(value: unknown, fallback: number) {
  const number = Number(value ?? fallback);
  return Number.isFinite(number) ? number : fallback;
}
function positive(value: unknown, fallback: number) {
  return Math.max(1, Math.round(finite(value, fallback)));
}
function nonnegative(value: unknown, fallback: number) {
  return Math.max(0, Math.round(finite(value, fallback)));
}
function range(value: unknown, min: number, max: number, fallback: number) {
  return Math.min(max, Math.max(min, finite(value, fallback)));
}
function filterText(value: string) {
  return value
    .slice(0, 500)
    .replaceAll("\\", "\\\\")
    .replaceAll("'", "\\'")
    .replaceAll(":", "\\:")
    .replaceAll("%", "\\%");
}
function filterPath(value: string) {
  return value
    .replaceAll("\\", "/")
    .replaceAll("'", "\\'")
    .replaceAll(":", "\\:");
}
function position(value: unknown, fallback: string) {
  if (typeof value === "number" && Number.isFinite(value))
    return String(Math.max(0, Math.round(value)));
  const allowed = new Set([
    "0",
    "16",
    "W-w-16",
    "H-h-16",
    "(w-text_w)/2",
    "h-text_h-24",
  ]);
  return typeof value === "string" && allowed.has(value) ? value : fallback;
}
function color(value: unknown, fallback: string) {
  return typeof value === "string" &&
    /^(#[0-9a-fA-F]{6}|[a-zA-Z]{3,20})$/.test(value)
    ? value
    : fallback;
}
function preset(value: unknown) {
  const allowed = new Set([
    "ultrafast",
    "superfast",
    "veryfast",
    "faster",
    "fast",
    "medium",
    "slow",
    "slower",
    "veryslow",
  ]);
  return typeof value === "string" && allowed.has(value) ? value : "medium";
}

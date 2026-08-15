import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MemoryResourceRepository } from "../src/repository.js";
import { ResourceService, type ProcessRunner } from "../src/service.js";
describe("Resource Center", () => {
  const roots: string[] = [];
  const services: ResourceService[] = [];
  afterEach(async () => {
    await Promise.all(services.splice(0).map((service) => service.shutdown()));
    await Promise.all(
      roots.splice(0).map((x) => rm(x, { recursive: true, force: true })),
    );
  });
  async function setup(runner?: ProcessRunner) {
    const root = await mkdtemp(join(tmpdir(), "qft-res-"));
    roots.push(root);
    const repo = new MemoryResourceRepository(),
      s = new ResourceService(repo, root, "ffmpeg", runner);
    services.push(s);
    await s.init();
    return { repo, s, root };
  }
  it("deduplicates content inside a tenant and isolates tenants", async () => {
    const { s } = await setup(),
      data = Buffer.from("same");
    const a = await s.put({
        tenantId: "t1",
        kind: "cache",
        name: "a",
        mediaType: "text/plain",
        data,
      }),
      b = await s.put({
        tenantId: "t1",
        kind: "cache",
        name: "b",
        mediaType: "text/plain",
        data,
      }),
      c = await s.put({
        tenantId: "t2",
        kind: "cache",
        name: "c",
        mediaType: "text/plain",
        data,
      });
    expect(a.created).toBe(true);
    expect(b.created).toBe(false);
    expect(b.item.id).toBe(a.item.id);
    expect(b.item.refs).toBe(2);
    expect(c.item.id).not.toBe(a.item.id);
    await expect(s.read(a.item.id, "t2")).rejects.toThrow("not found");
  });
  it("merges Bot ACLs on deduplication and enforces scoped reads", async () => {
    const { s } = await setup();
    const first = await s.put({
      tenantId: "t1",
      botId: "bot-a",
      kind: "artifact",
      name: "shared.txt",
      mediaType: "text/plain",
      data: Buffer.from("shared"),
    });
    const second = await s.put({
      tenantId: "t1",
      botId: "bot-b",
      kind: "artifact",
      name: "shared-copy.txt",
      mediaType: "text/plain",
      data: Buffer.from("shared"),
    });
    expect(second.item.id).toBe(first.item.id);
    expect(second.item.allowedBotIds).toEqual(["bot-a", "bot-b"]);
    expect((await s.read(first.item.id, "t1", "bot-b")).data.toString()).toBe(
      "shared",
    );
    await expect(s.read(first.item.id, "t1", "bot-c")).rejects.toThrow(
      "not found",
    );
    await s.setAcl(first.item.id, "t1", ["bot-a"]);
    await expect(s.read(first.item.id, "t1", "bot-b")).rejects.toThrow(
      "not found",
    );
  });
  it("reports storage statistics and detects missing or corrupt objects", async () => {
    const { repo, s } = await setup();
    const healthy = await s.put({
      tenantId: "t1",
      botId: "bot-a",
      kind: "cache",
      name: "healthy.txt",
      mediaType: "text/plain",
      data: Buffer.from("healthy"),
    });
    const corrupt = await s.put({
      tenantId: "t1",
      kind: "artifact",
      name: "corrupt.txt",
      mediaType: "text/plain",
      data: Buffer.from("original"),
    });
    const missing = await s.put({
      tenantId: "t1",
      kind: "log",
      name: "missing.log",
      mediaType: "text/plain",
      data: Buffer.from("missing"),
    });
    await writeFile(corrupt.item.path, Buffer.from("changed"));
    await rm(missing.item.path);
    const stats = await s.stats("t1");
    expect(stats).toMatchObject({ count: 3, referenced: 3 });
    expect(stats.byKind.cache.count).toBe(1);
    const checked = await s.checkIntegrity({ tenantId: "t1", dryRun: true });
    expect(checked).toMatchObject({ healthy: 1, corrupt: 1, missing: 1 });
    const repaired = await s.checkIntegrity({
      tenantId: "t1",
      dryRun: false,
      removeMissing: true,
    });
    expect(repaired.results.filter((item) => item.repaired)).toHaveLength(2);
    expect(await repo.get(missing.item.id)).toBeUndefined();
    expect((await repo.get(corrupt.item.id))?.metadata.integrity).toMatchObject(
      {
        status: "corrupt",
      },
    );
    expect(await repo.get(healthy.item.id)).toBeTruthy();
  });
  it("does not clean referenced resources", async () => {
    const { repo, s } = await setup(),
      made = await s.put({
        tenantId: "t1",
        kind: "cache",
        name: "a",
        mediaType: "text/plain",
        data: Buffer.from("x"),
      });
    made.item.lastAccessedAt = "2020-01-01T00:00:00.000Z";
    await repo.save(made.item);
    const p = await s.planCleanup({
      tenantId: "t1",
      olderThan: "2021-01-01T00:00:00.000Z",
      dryRun: false,
    });
    expect(p.candidates).toHaveLength(0);
    made.item.refs = 0;
    await repo.save(made.item);
    const p2 = await s.planCleanup({
      tenantId: "t1",
      olderThan: "2021-01-01T00:00:00.000Z",
      dryRun: false,
    });
    await s.executeCleanup(p2.id);
    expect(await repo.get(made.item.id)).toBeUndefined();
  });
  it("runs media with controlled arguments and stores output", async () => {
    let seen: string[] = [];
    const runner: ProcessRunner = {
        run: async (_c, args) => {
          seen = args;
          await writeFile(args.at(-1)!, Buffer.from("video-output"));
        },
      },
      { s } = await setup(runner),
      input = await s.put({
        tenantId: "t1",
        kind: "object",
        name: "in.mp4",
        mediaType: "video/mp4",
        data: Buffer.from("input"),
      }),
      result = await s.runMedia({
        tenantId: "t1",
        operation: "thumbnail",
        inputIds: [input.item.id],
        outputName: "thumb.jpg",
        mediaType: "image/jpeg",
        params: { at: "00:00:02" },
      });
    expect(seen).toContain("-frames:v");
    expect(seen.at(-1)).toMatch(/\.jpg$/);
    expect(result.job.status).toBe("succeeded");
    expect((await s.read(result.output!.id, "t1")).data.toString()).toBe(
      "video-output",
    );
  });
  it("queues immediately, enforces concurrency and reports progress", async () => {
    const releases: Array<() => void> = [];
    let active = 0,
      maximum = 0;
    const runner: ProcessRunner = {
      run: async (_command, args, options) => {
        active += 1;
        maximum = Math.max(maximum, active);
        options?.onProgress?.(42);
        await new Promise<void>((resolve) => releases.push(resolve));
        active -= 1;
        await writeFile(args.at(-1)!, Buffer.from("done"));
      },
    };
    const { s } = await setup(runner);
    const input = await s.put({
      tenantId: "t1",
      kind: "object",
      name: "in.mp4",
      mediaType: "video/mp4",
      data: Buffer.from("input"),
    });
    const request = {
      tenantId: "t1",
      operation: "thumbnail",
      inputIds: [input.item.id],
      outputName: "thumb.jpg",
      mediaType: "image/jpeg",
      params: {},
    };
    const first = await s.enqueueMedia(request);
    const second = await s.enqueueMedia(request);
    expect(first.status).toBe("queued");
    expect(second.status).toBe("queued");
    await until(
      async () => (await s.getMediaJob(first.id)).status === "running",
    );
    expect((await s.getMediaJob(second.id)).status).toBe("queued");
    await until(async () => (await s.getMediaJob(first.id)).progress >= 42);
    releases.shift()?.();
    await until(
      async () => (await s.getMediaJob(second.id)).status === "running",
    );
    releases.shift()?.();
    expect((await s.waitForMediaJob(second.id)).status).toBe("succeeded");
    expect(maximum).toBe(1);
  });
  it("cancels a running process and permits an explicit retry", async () => {
    let runs = 0;
    const runner: ProcessRunner = {
      run: async (_command, args, options) => {
        runs += 1;
        if (runs === 1)
          await new Promise<void>((_resolve, reject) =>
            options?.signal?.addEventListener(
              "abort",
              () =>
                reject(
                  Object.assign(new Error("cancelled"), { name: "AbortError" }),
                ),
              { once: true },
            ),
          );
        else await writeFile(args.at(-1)!, Buffer.from("retried"));
      },
    };
    const { s } = await setup(runner);
    const input = await s.put({
      tenantId: "t1",
      kind: "object",
      name: "in.mp4",
      mediaType: "video/mp4",
      data: Buffer.from("input"),
    });
    const queued = await s.enqueueMedia({
      tenantId: "t1",
      operation: "thumbnail",
      inputIds: [input.item.id],
      outputName: "thumb.jpg",
      mediaType: "image/jpeg",
      params: {},
    });
    await until(
      async () => (await s.getMediaJob(queued.id)).status === "running",
    );
    await s.cancelMediaJob(queued.id, "t1");
    expect((await s.waitForMediaJob(queued.id)).status).toBe("cancelled");
    await s.retryMediaJob(queued.id, "t1");
    const completed = await s.waitForMediaJob(queued.id);
    expect(completed.status).toBe("succeeded");
    expect(completed.attempts).toBe(2);
  });
  it("recovers an interrupted persisted job on startup", async () => {
    const root = await mkdtemp(join(tmpdir(), "qft-res-"));
    roots.push(root);
    const repo = new MemoryResourceRepository();
    const inputService = new ResourceService(repo, root, "ffmpeg");
    services.push(inputService);
    await inputService.init();
    const input = await inputService.put({
      tenantId: "t1",
      kind: "object",
      name: "in.mp4",
      mediaType: "video/mp4",
      data: Buffer.from("input"),
    });
    await inputService.shutdown();
    services.splice(services.indexOf(inputService), 1);
    const createdAt = new Date().toISOString();
    await repo.saveJob({
      id: "00000000-0000-4000-8000-000000000001",
      tenantId: "t1",
      operation: "thumbnail",
      inputIds: [input.item.id],
      args: ["[RESOURCE]", "[OUTPUT]"],
      status: "running",
      progress: 60,
      attempts: 1,
      recoveryCount: 0,
      cancelRequested: false,
      request: {
        outputName: "recovered.jpg",
        mediaType: "image/jpeg",
        params: {},
      },
      createdAt,
      updatedAt: createdAt,
      startedAt: createdAt,
    });
    const runner: ProcessRunner = {
      run: async (_command, args) =>
        writeFile(args.at(-1)!, Buffer.from("recovered")),
    };
    const recovered = new ResourceService(repo, root, "ffmpeg", runner);
    services.push(recovered);
    await recovered.init();
    const job = await recovered.waitForMediaJob(
      "00000000-0000-4000-8000-000000000001",
    );
    expect(job.status).toBe("succeeded");
    expect(job.recoveryCount).toBe(1);
    expect(job.attempts).toBe(2);
  });
  it("builds the complete media operation set without shell execution", async () => {
    const { s } = await setup();
    const input = "/managed/input.mp4";
    expect(s.mediaArgs("crop", [input], "/managed/out.mp4", {})).toContain(
      "crop=640:360:0:0",
    );
    expect(
      s.mediaArgs(
        "watermark",
        [input, "/managed/logo.png"],
        "/managed/out.mp4",
        {},
      ),
    ).toContain("overlay=W-w-16:H-h-16");
    expect(
      s.mediaArgs("compress", [input], "/managed/out.mp4", { crf: 99 }),
    ).toContain("51");
    expect(() =>
      s.mediaArgs("rotate", [input], "/managed/out.mp4", { degrees: 13 }),
    ).toThrow("Rotate supports");
  });
  it("creates a sanitized diagnostics zip", async () => {
    const { s } = await setup(),
      result = await s.diagnostics({
        tenantId: "t1",
        sections: { config: { apiKey: "secret", safe: "yes" } },
        logs: [{ name: "app.log", content: "Bearer abcdef" }],
      });
    expect(result.item.mediaType).toBe("application/zip");
    expect(result.item.size).toBeGreaterThan(0);
  });
});

async function until(check: () => Promise<boolean>, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("condition not reached");
}

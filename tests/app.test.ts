import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { buildApp } from "../src/app.js";
import { MemoryResourceRepository } from "../src/repository.js";
import type { ProcessRunner } from "../src/service.js";

describe("Resource Center media API", () => {
  const cleanup: Array<() => Promise<void>> = [];
  afterEach(async () => {
    await Promise.allSettled(cleanup.splice(0).map((run) => run()));
  });

  it("returns an asynchronous job and supports get, cancel and retry", async () => {
    const root = await mkdtemp(join(tmpdir(), "qft-res-api-"));
    const inputPath = join(root, "input.wav");
    await writeFile(inputPath, Buffer.from("input"));
    const repo = new MemoryResourceRepository();
    const timestamp = new Date().toISOString();
    const inputId = "00000000-0000-4000-8000-000000000010";
    await repo.save({
      id: inputId,
      tenantId: "tenant-a",
      kind: "object",
      sha256: "hash",
      size: 5,
      mediaType: "audio/wav",
      name: "input.wav",
      path: inputPath,
      refs: 1,
      metadata: {},
      createdAt: timestamp,
      lastAccessedAt: timestamp,
    });
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
        else await writeFile(args.at(-1)!, Buffer.from("output"));
      },
    };
    const app = buildApp({
      repository: repo,
      internalToken: "test-token",
      root,
      ffmpeg: "ffmpeg",
      runner,
    });
    await app.ready();
    cleanup.push(
      async () => app.close(),
      async () => rm(root, { recursive: true, force: true }),
    );

    const created = await app.inject({
      method: "POST",
      url: "/v1/media/jobs",
      headers: { authorization: "Bearer test-token" },
      payload: {
        tenantId: "tenant-a",
        operation: "normalize",
        inputIds: [inputId],
        outputName: "output.wav",
        mediaType: "audio/wav",
        params: {},
      },
    });
    expect(created.statusCode).toBe(202);
    const job = created.json().data;
    expect(job.status).toBe("queued");
    await until(async () => (await repo.job(job.id))?.status === "running");

    const denied = await app.inject({
      method: "GET",
      url: `/v1/media/jobs/${job.id}?tenantId=tenant-b`,
      headers: { authorization: "Bearer test-token" },
    });
    expect(denied.statusCode).toBe(404);

    const cancelled = await app.inject({
      method: "POST",
      url: `/v1/media/jobs/${job.id}/cancel`,
      headers: { authorization: "Bearer test-token" },
      payload: { tenantId: "tenant-a" },
    });
    expect(cancelled.statusCode).toBe(200);
    await until(async () => (await repo.job(job.id))?.status === "cancelled");

    const retried = await app.inject({
      method: "POST",
      url: `/v1/media/jobs/${job.id}/retry`,
      headers: { authorization: "Bearer test-token" },
      payload: { tenantId: "tenant-a" },
    });
    expect(retried.statusCode).toBe(202);
    await until(async () => (await repo.job(job.id))?.status === "succeeded");
    expect((await repo.job(job.id))?.attempts).toBe(2);
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

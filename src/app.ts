import Fastify from "fastify";
import multipart from "@fastify/multipart";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import type { ResourceRepository } from "./repository.js";
import { ResourceService, type ProcessRunner } from "./service.js";
import type { ExtensionStateRepository } from "./extensions.js";
const ok = (data: unknown, id: string) => ({ ok: true, data, requestId: id }),
  fail = (message: string, id: string) => ({
    ok: false,
    error: { code: "REQUEST_FAILED", message },
    requestId: id,
  });
const fieldValue = (field: unknown) => {
  if (
    field &&
    !Array.isArray(field) &&
    typeof field === "object" &&
    "value" in field
  )
    return String((field as { value: unknown }).value);
  return undefined;
};
export function buildApp(o: {
  repository: ResourceRepository;
  internalToken: string;
  root: string;
  ffmpeg: string;
  ffprobe?: string;
  runner?: ProcessRunner;
  mediaConcurrency?: number;
  extensionRepository?: ExtensionStateRepository;
}) {
  const app = Fastify({ logger: false, genReqId: () => randomUUID() }),
    s = new ResourceService(
      o.repository,
      o.root,
      o.ffmpeg,
      o.runner,
      o.ffprobe,
      o.mediaConcurrency,
      o.extensionRepository,
    );
  app.addHook("onReady", async () => s.extensions.initialize());
  app.addHook("onClose", async () => s.extensions.close());
  void app.register(multipart, {
    limits: { fileSize: 512 * 1024 * 1024, files: 1 },
  });
  app.addHook("onRequest", async (req, reply) => {
    if (["/healthz", "/readyz", "/version"].includes(req.url)) return;
    if (req.headers.authorization !== `Bearer ${o.internalToken}`)
      return reply.code(401).send(fail("Invalid service token", req.id));
  });
  app.setErrorHandler((e: any, req, reply) =>
    reply.code(e.statusCode ?? 500).send(fail(e.message, req.id)),
  );
  app.get("/healthz", async (req) =>
    ok({ service: "resource-center", status: "ok" }, req.id),
  );
  app.get("/readyz", async (req, reply) => {
    const r = await o.repository.ping();
    return reply.code(r ? 200 : 503).send(ok({ ready: r }, req.id));
  });
  app.get("/version", async (req) => ok({ version: "0.1.0" }, req.id));
  app.get("/v1/extensions", async (req) => ok(s.extensions.list(), req.id));
  app.get("/v1/extensions/:id", async (req) =>
    ok(
      s.extensions.get(z.object({ id: z.string() }).parse(req.params).id),
      req.id,
    ),
  );
  app.post("/v1/extensions/:id/probe", async (req) =>
    ok(
      await s.extensions.probe(
        z.object({ id: z.string() }).parse(req.params).id,
      ),
      req.id,
    ),
  );
  app.post("/v1/extensions/:id/lifecycle/:state", async (req) => {
    const { id, state } = z
      .object({
        id: z.string(),
        state: z.enum([
          "installed",
          "verified",
          "canary",
          "active",
          "draining",
          "disabled",
          "failed",
          "retired",
        ]),
      })
      .parse(req.params);
    return ok(await s.extensions.transition(id, state), req.id);
  });
  app.get("/v1/extensions/:id/logs", async (req) =>
    ok(
      await s.extensions.logs(
        z.object({ id: z.string() }).parse(req.params).id,
      ),
      req.id,
    ),
  );
  app.post("/v1/resources", async (req, reply) => {
    const file = await req.file();
    if (!file) return reply.code(400).send(fail("File required", req.id));
    const tenantId = fieldValue(file.fields.tenantId) ?? "",
      kind = z
        .enum(["object", "cache", "artifact", "log", "workspace", "diagnostic"])
        .parse(fieldValue(file.fields.kind) ?? "object"),
      botId = fieldValue(file.fields.botId),
      data = await file.toBuffer();
    return reply.code(201).send(
      ok(
        await s.put({
          tenantId,
          botId,
          kind,
          name: file.filename,
          mediaType: file.mimetype,
          data,
        }),
        req.id,
      ),
    );
  });
  app.get("/v1/resources", async (req) => {
    const q = z
      .object({ tenantId: z.string().optional(), botId: z.string().optional() })
      .parse(req.query);
    return ok(await s.list(q.tenantId, q.botId), req.id);
  });
  app.get("/v1/resources/:id/content", async (req, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params),
      { tenantId, botId } = z
        .object({ tenantId: z.string(), botId: z.string().optional() })
        .parse(req.query),
      r = await s.read(id, tenantId, botId);
    return reply.type(r.item.mediaType).send(r.data);
  });
  app.patch("/v1/resources/:id/acl", async (req) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const body = z
      .object({
        tenantId: z.string().min(1),
        allowedBotIds: z.array(z.string().min(1).max(200)).max(100),
      })
      .parse(req.body);
    return ok(await s.setAcl(id, body.tenantId, body.allowedBotIds), req.id);
  });
  app.post("/v1/resources/:id/retain", async (req) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const { tenantId } = z.object({ tenantId: z.string() }).parse(req.body);
    return ok(await s.retain(id, tenantId, 1), req.id);
  });
  app.post("/v1/resources/:id/release", async (req) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const { tenantId } = z.object({ tenantId: z.string() }).parse(req.body);
    return ok(await s.retain(id, tenantId, -1), req.id);
  });
  app.get("/v1/stats", async (req) => {
    const { tenantId } = z.object({ tenantId: z.string() }).parse(req.query);
    return ok(await s.stats(tenantId), req.id);
  });
  app.post("/v1/integrity/check", async (req) =>
    ok(
      await s.checkIntegrity(
        z
          .object({
            tenantId: z.string(),
            dryRun: z.boolean().default(true),
            removeMissing: z.boolean().default(false),
          })
          .parse(req.body),
      ),
      req.id,
    ),
  );
  app.post("/v1/cleanup/plans", async (req, reply) =>
    reply.code(201).send(
      ok(
        await s.planCleanup(
          z
            .object({
              tenantId: z.string(),
              olderThan: z.string().datetime(),
              kinds: z
                .array(
                  z.enum([
                    "object",
                    "cache",
                    "artifact",
                    "log",
                    "workspace",
                    "diagnostic",
                  ]),
                )
                .optional(),
              dryRun: z.boolean().optional(),
            })
            .parse(req.body),
        ),
        req.id,
      ),
    ),
  );
  app.post("/v1/cleanup/plans/:id/execute", async (req) =>
    ok(
      await s.executeCleanup(
        z.object({ id: z.string().uuid() }).parse(req.params).id,
      ),
      req.id,
    ),
  );
  app.post("/v1/diagnostics", async (req, reply) =>
    reply.code(201).send(
      ok(
        await s.diagnostics(
          z
            .object({
              tenantId: z.string(),
              sections: z.record(z.string(), z.unknown()),
              logs: z
                .array(
                  z.object({
                    name: z.string().min(1).max(200),
                    content: z.string().max(200_000),
                  }),
                )
                .max(50)
                .optional(),
            })
            .parse(req.body),
        ),
        req.id,
      ),
    ),
  );
  app.post("/v1/media/jobs", async (req, reply) =>
    reply.code(202).send(
      ok(
        await s.enqueueMedia(
          z
            .object({
              tenantId: z.string(),
              botId: z.string().optional(),
              operation: z.enum([
                "info",
                "transcode",
                "export",
                "thumbnail",
                "extract-audio",
                "trim",
                "scale",
                "resize",
                "merge",
                "crop",
                "rotate",
                "color",
                "audio",
                "text",
                "subtitle",
                "speed",
                "compress",
                "normalize",
                "fade",
                "watermark",
                "gif",
                "blur",
              ]),
              inputIds: z.array(z.string().uuid()).min(1),
              outputName: z.string(),
              mediaType: z.string(),
              params: z.record(z.string(), z.unknown()).default({}),
            })
            .parse(req.body),
        ),
        req.id,
      ),
    ),
  );
  app.get("/v1/media/jobs", async (req) => {
    const q = z.object({ tenantId: z.string().optional() }).parse(req.query);
    return ok(await o.repository.jobs(q.tenantId), req.id);
  });
  app.get("/v1/media/jobs/:id", async (req) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const { tenantId } = z
      .object({ tenantId: z.string().optional() })
      .parse(req.query);
    return ok(await s.getMediaJob(id, tenantId), req.id);
  });
  app.post("/v1/media/jobs/:id/cancel", async (req) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const { tenantId } = z
      .object({ tenantId: z.string().optional() })
      .parse(req.body ?? {});
    return ok(await s.cancelMediaJob(id, tenantId), req.id);
  });
  app.post("/v1/media/jobs/:id/retry", async (req, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(req.params);
    const { tenantId } = z
      .object({ tenantId: z.string().optional() })
      .parse(req.body ?? {});
    return reply
      .code(202)
      .send(ok(await s.retryMediaJob(id, tenantId), req.id));
  });
  app.addHook("onReady", async () => s.init());
  app.addHook("onClose", async () => {
    await s.shutdown();
    await o.repository.close();
  });
  return app;
}

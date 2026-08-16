import {
  ExtensionCatalog,
  type ExtensionStateRepository,
} from "./extension-catalog.js";

export * from "./extension-catalog.js";

export const createResourceExtensions = (
  repository?: ExtensionStateRepository,
) =>
  new ExtensionCatalog(
    [
      {
        providerId: "resource-backend.local-fs",
        family: "resource-backend",
        version: "1.0.0",
        contractVersion: "1.0",
        displayName: "Local Filesystem Backend",
        isolation: "process",
        capabilities: {
          objects: true,
          acl: true,
          retention: true,
          integrity: true,
        },
      },
      {
        providerId: "resource-processor.diagnostics-zip",
        family: "resource-processor",
        version: "1.0.0",
        contractVersion: "1.0",
        displayName: "Diagnostic Bundle Processor",
        isolation: "in-process",
        capabilities: { zip: true, redaction: true },
      },
      {
        providerId: "resource-processor.ffmpeg",
        family: "resource-processor",
        version: "1.0.0",
        contractVersion: "1.0",
        displayName: "FFmpeg Media Processor",
        isolation: "process",
        capabilities: { audio: true, video: true, gif: true, probe: true },
      },
    ],
    repository,
  );

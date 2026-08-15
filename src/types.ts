export type ResourceKind =
  "object" | "cache" | "artifact" | "log" | "workspace" | "diagnostic";
export interface ResourceItem {
  id: string;
  tenantId: string;
  botId?: string;
  allowedBotIds?: string[];
  kind: ResourceKind;
  sha256: string;
  size: number;
  mediaType: string;
  name: string;
  path: string;
  refs: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  lastAccessedAt: string;
  expiresAt?: string;
}
export interface CleanupPlan {
  id: string;
  tenantId: string;
  dryRun: boolean;
  candidates: Array<{ id: string; size: number; reason: string }>;
  totalBytes: number;
  createdAt: string;
  executedAt?: string;
}
export interface MediaJob {
  id: string;
  tenantId: string;
  botId?: string;
  operation: string;
  inputIds: string[];
  outputId?: string;
  args: string[];
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  progress: number;
  attempts: number;
  recoveryCount: number;
  cancelRequested: boolean;
  request: {
    outputName: string;
    mediaType: string;
    params: Record<string, unknown>;
  };
  error?: string;
  result?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  finishedAt?: string;
}

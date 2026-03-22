import { z } from "zod";
import type { Prisma } from "@/generated/prisma/client";

export type NormalizedOffer = {
  offerId: string;
  hostId: number | null;
  machineId: number | null;
  gpuName: string;
  numGpus: number;
  gpuRamGb: number | null;
  cpuCores: number | null;
  ramGb: number | null;
  diskGb: number | null;
  inetUpMbps: number | null;
  inetDownMbps: number | null;
  rentable: boolean;
  rented: boolean;
  verified: boolean | null;
  reliabilityScore: number | null;
  pricePerHour: number | null;
  rawJson: Prisma.InputJsonValue;
};

const rawResponseSchema = z.union([
  z.array(z.unknown()),
  z.object({
    offers: z.union([z.array(z.unknown()), z.unknown()]),
  }),
]);

function parseNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(normalized)) return true;
    if (["false", "0", "no", "n"].includes(normalized)) return false;
  }
  return null;
}

function pickFirstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const parsed = parseNumber(value);
    if (parsed !== null) return parsed;
  }
  return null;
}

function pickFirstBoolean(...values: unknown[]): boolean | null {
  for (const value of values) {
    const parsed = parseBoolean(value);
    if (parsed !== null) return parsed;
  }
  return null;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
}

function normalizeOffer(raw: unknown, index: number): NormalizedOffer | null {
  const record = asRecord(raw);
  const gpu = asRecord(record.gpu);
  const network = asRecord(record.network);

  const gpuName =
    (record.gpu_name as string | undefined) ??
    (record.gpuName as string | undefined) ??
    (gpu.name as string | undefined) ??
    (record.model as string | undefined);

  if (!gpuName || gpuName.trim().length === 0) {
    return null;
  }

  const offerId =
    String(record.id ?? record.offer_id ?? record.offerId ?? `${gpuName.toLowerCase()}-${index}`);
  const rawHostId = pickFirstNumber(record.host_id, record.hostId);
  const rawMachineId = pickFirstNumber(record.machine_id, record.machineId);

  const numGpus = pickFirstNumber(
    record.num_gpus,
    record.numGpus,
    record.gpu_count,
    record.gpuCount,
    gpu.count,
  );

  const rentable = pickFirstBoolean(record.rentable, record.is_rentable, record.available);
  const rented = pickFirstBoolean(record.rented, record.is_rented, record.in_use);

  return {
    offerId,
    hostId: rawHostId == null ? null : Math.trunc(rawHostId),
    machineId: rawMachineId == null ? null : Math.trunc(rawMachineId),
    gpuName,
    numGpus: Math.max(1, Math.trunc(numGpus ?? 1)),
    gpuRamGb: pickFirstNumber(record.gpu_ram, record.gpuRamGb, gpu.ram_gb, gpu.ram),
    cpuCores: pickFirstNumber(record.cpu_cores, record.cpuCores),
    ramGb: pickFirstNumber(record.ram_gb, record.ramGb),
    diskGb: pickFirstNumber(record.disk_gb, record.diskGb),
    inetUpMbps: pickFirstNumber(record.inet_up_mbps, record.inetUpMbps, network.up_mbps),
    inetDownMbps: pickFirstNumber(record.inet_down_mbps, record.inetDownMbps, network.down_mbps),
    rentable: rentable ?? false,
    rented: rented ?? false,
    verified: pickFirstBoolean(record.verified, record.is_verified),
    reliabilityScore: pickFirstNumber(record.reliability_score, record.reliabilityScore),
    pricePerHour: pickFirstNumber(
      record.price_per_hour,
      record.pricePerHour,
      record.dph_total,
      record.hourly_price,
    ),
    rawJson: record as Prisma.InputJsonValue,
  };
}

export async function collectVastOffers(): Promise<NormalizedOffer[]> {
  const endpoint =
    process.env.VAST_API_URL?.trim() || "https://console.vast.ai/api/v0/bundles/";
  const method = (process.env.VAST_API_METHOD?.trim() || "POST").toUpperCase();
  try {
    new URL(endpoint);
  } catch {
    throw new Error(
      `Invalid VAST_API_URL: "${endpoint}". Set VAST_API_URL to a full URL or unset it to use default.`,
    );
  }

  const headers: Record<string, string> = {
    Accept: "application/json",
  };

  if (process.env.VAST_API_KEY) {
    headers.Authorization = `Bearer ${process.env.VAST_API_KEY}`;
  }

  const defaultQuery = {
    limit: 100,
    type: "on-demand",
    verified: { eq: true },
  };

  let body: string | undefined;
  if (method !== "GET") {
    headers["Content-Type"] = "application/json";
    body = process.env.VAST_REQUEST_JSON
      ? process.env.VAST_REQUEST_JSON
      : JSON.stringify(defaultQuery);
  }

  const response = await fetch(endpoint, {
    method,
    headers,
    body,
    signal: AbortSignal.timeout(15000),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Vast API request failed (${response.status} ${response.statusText})`);
  }

  const json = (await response.json()) as unknown;
  const parsed = rawResponseSchema.parse(json);
  const rows = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed.offers)
      ? parsed.offers
      : [parsed.offers];

  return rows
    .map((row, index) => normalizeOffer(row, index))
    .filter((row): row is NormalizedOffer => row !== null);
}

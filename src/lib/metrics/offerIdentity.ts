import { createHash } from "node:crypto";

export type OfferIdentityInput = {
  source: string;
  offerExternalId?: string | null;
  machineId?: number | null;
  hostId?: number | null;
  gpuName: string;
  numGpus: number;
  offerType?: string | null;
  gpuRamGb?: number | null;
  cpuCores?: number | null;
  ramGb?: number | null;
  diskGb?: number | null;
  reliabilityScore?: number | null;
  verified?: boolean | null;
  pricePerHour?: number | null;
  inetDownMbps?: number | null;
  inetUpMbps?: number | null;
  geolocation?: unknown;
  sourceFingerprint?: string | null;
};

export type OfferIdentityResult = {
  offerExternalId: string | null;
  fingerprint: string;
  stableOfferFingerprint: string;
  versionFingerprint: string;
  strategy: "external_id" | "machine_signature" | "host_signature" | "weak_signature";
  identityQualityScore: number;
};

function normalizeOfferType(offerType?: string | null): string {
  const normalized = offerType?.trim().toLowerCase();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

function round(value: number | null | undefined, precision = 4): string {
  if (value == null || !Number.isFinite(value)) return "na";
  return value.toFixed(precision);
}

function hashSegments(segments: Array<string | number>): string {
  const joined = segments.join("|");
  return createHash("sha256").update(joined).digest("hex").slice(0, 32);
}

function normalizeRegion(geolocation: unknown): string {
  if (!geolocation || typeof geolocation !== "object") return "geo:na";
  const record = geolocation as Record<string, unknown>;
  const country = typeof record.country === "string" ? record.country.toLowerCase() : "na";
  const region =
    typeof record.region === "string"
      ? record.region.toLowerCase()
      : typeof record.state === "string"
        ? record.state.toLowerCase()
        : "na";
  return `geo:${country}:${region}`;
}

function bucketizePrice(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "p:na";
  if (value < 0.5) return "p:low";
  if (value < 2) return "p:mid";
  return "p:high";
}

function networkTier(input: OfferIdentityInput): string {
  const down = input.inetDownMbps ?? 0;
  const up = input.inetUpMbps ?? 0;
  const score = Math.min(down, up);
  if (score >= 1000) return "net:t1";
  if (score >= 300) return "net:t2";
  if (score > 0) return "net:t3";
  return "net:na";
}

export function buildOfferIdentity(input: OfferIdentityInput): OfferIdentityResult {
  const mutableVersionSegments = [
    round(input.pricePerHour, 4),
    round(input.reliabilityScore, 3),
    round(input.inetDownMbps, 0),
    round(input.inetUpMbps, 0),
  ];

  const normalizedType = normalizeOfferType(input.offerType);
  const stableCommon = [
    input.source,
    input.gpuName.trim().toLowerCase(),
    String(input.numGpus),
    normalizedType,
    String(input.gpuRamGb ?? "na"),
    String(input.cpuCores ?? "na"),
    String(input.ramGb ?? "na"),
    String(input.diskGb ?? "na"),
    input.verified == null ? "na" : input.verified ? "v1" : "v0",
    normalizeRegion(input.geolocation),
    networkTier(input),
    input.sourceFingerprint?.trim().toLowerCase() || "fp:na",
  ];

  const preferMachineIdentity = input.source.toLowerCase().includes("vast") && input.machineId != null;

  if (input.machineId != null && preferMachineIdentity) {
    const stableOfferFingerprint = `mach:${hashSegments([`m${input.machineId}`, ...stableCommon])}`;
    const versionFingerprint = `machv:${hashSegments([
      `m${input.machineId}`,
      ...stableCommon,
      ...mutableVersionSegments,
    ])}`;
    return {
      offerExternalId: null,
      fingerprint: stableOfferFingerprint,
      stableOfferFingerprint,
      versionFingerprint,
      strategy: "machine_signature",
      identityQualityScore: 0.9,
    };
  }

  if (input.offerExternalId && input.offerExternalId.trim().length > 0) {
    const externalId = input.offerExternalId.trim();
    const stableOfferFingerprint = `ext:${input.source}:${externalId}`;
    const versionFingerprint = `extv:${hashSegments([
      input.source,
      externalId,
      ...mutableVersionSegments,
    ])}`;
    return {
      offerExternalId: externalId,
      fingerprint: stableOfferFingerprint,
      stableOfferFingerprint,
      versionFingerprint,
      strategy: "external_id",
      identityQualityScore: 1,
    };
  }

  if (input.machineId != null) {
    const stableOfferFingerprint = `mach:${hashSegments([`m${input.machineId}`, ...stableCommon])}`;
    const versionFingerprint = `machv:${hashSegments([
      `m${input.machineId}`,
      ...stableCommon,
      ...mutableVersionSegments,
    ])}`;
    return {
      offerExternalId: null,
      fingerprint: stableOfferFingerprint,
      stableOfferFingerprint,
      versionFingerprint,
      strategy: "machine_signature",
      identityQualityScore: 0.9,
    };
  }

  if (input.hostId != null) {
    const stableOfferFingerprint = `host:${hashSegments([`h${input.hostId}`, ...stableCommon])}`;
    const versionFingerprint = `hostv:${hashSegments([
      `h${input.hostId}`,
      ...stableCommon,
      ...mutableVersionSegments,
    ])}`;
    return {
      offerExternalId: null,
      fingerprint: stableOfferFingerprint,
      stableOfferFingerprint,
      versionFingerprint,
      strategy: "host_signature",
      identityQualityScore: 0.76,
    };
  }

  const weakStableCommon = [...stableCommon, bucketizePrice(input.pricePerHour)];
  const stableOfferFingerprint = `weak:${hashSegments(weakStableCommon)}`;
  const versionFingerprint = `weakv:${hashSegments([...weakStableCommon, ...mutableVersionSegments])}`;
  return {
    offerExternalId: null,
    fingerprint: stableOfferFingerprint,
    stableOfferFingerprint,
    versionFingerprint,
    strategy: "weak_signature",
    identityQualityScore: 0.5,
  };
}

export function detectFingerprintCollisions(
  identities: Array<{ fingerprint: string; signature: string }>,
): Map<string, Set<string>> {
  const grouped = new Map<string, Set<string>>();
  for (const identity of identities) {
    const set = grouped.get(identity.fingerprint) ?? new Set<string>();
    set.add(identity.signature);
    grouped.set(identity.fingerprint, set);
  }

  const collisions = new Map<string, Set<string>>();
  for (const [fingerprint, signatures] of grouped.entries()) {
    if (signatures.size > 1) {
      collisions.set(fingerprint, signatures);
    }
  }

  return collisions;
}

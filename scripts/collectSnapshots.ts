import "dotenv/config";
import { createHash } from "node:crypto";
import { PrismaClient, type Prisma } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { collectVastOffers } from "../src/lib/vast/collector";
import { buildOfferIdentity, detectFingerprintCollisions } from "../src/lib/metrics/offerIdentity";

type SnapshotOfferInput = {
  offerId: string;
  hostId?: number | null;
  machineId?: number | null;
  gpuName: string;
  numGpus: number;
  offerType?: string | null;
  gpuRamGb: number | null;
  cpuCores?: number | null;
  ramGb?: number | null;
  diskGb?: number | null;
  inetUpMbps?: number | null;
  inetDownMbps?: number | null;
  rentable: boolean;
  rented: boolean;
  verified?: boolean | null;
  reliabilityScore?: number | null;
  pricePerHour: number | null;
  dlperf?: number | null;
  timeRemaining?: number | null;
  geolocation?: Prisma.InputJsonValue;
  rawJson: Prisma.InputJsonValue;
};

const mockOffers: SnapshotOfferInput[] = [
  {
    offerId: "vast-mock-rtx4000ada-1",
    hostId: 1101,
    machineId: 2101,
    gpuName: "RTX 4000 Ada",
    numGpus: 1,
    offerType: "on-demand",
    gpuRamGb: 20,
    rentable: true,
    rented: false,
    pricePerHour: 0.62,
    rawJson: { provider: "mock", city: "Denver", verified: true },
  },
  {
    offerId: "vast-mock-rtx4000ada-2",
    hostId: 1102,
    machineId: 2102,
    gpuName: "RTX 4000 Ada",
    numGpus: 1,
    offerType: "on-demand",
    gpuRamGb: 20,
    rentable: false,
    rented: true,
    pricePerHour: 0.69,
    rawJson: { provider: "mock", city: "Austin", verified: true },
  },
  {
    offerId: "vast-mock-l4-1",
    hostId: 1201,
    machineId: 2201,
    gpuName: "NVIDIA L4",
    numGpus: 1,
    offerType: "on-demand",
    gpuRamGb: 24,
    rentable: true,
    rented: false,
    pricePerHour: 0.74,
    rawJson: { provider: "mock", city: "Phoenix", verified: false },
  },
  {
    offerId: "vast-mock-l4-2",
    hostId: 1202,
    machineId: 2202,
    gpuName: "NVIDIA L4",
    numGpus: 1,
    offerType: "on-demand",
    gpuRamGb: 24,
    rentable: false,
    rented: true,
    pricePerHour: 0.81,
    rawJson: { provider: "mock", city: "Seattle", verified: true },
  },
];

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

async function resolveOffers(): Promise<{ source: string; offers: SnapshotOfferInput[] }> {
  const mode = (process.env.INGEST_MODE ?? "mock").toLowerCase();

  if (mode === "vast") {
    const offers = await collectVastOffers();
    if (offers.length === 0) {
      throw new Error("Vast ingestion returned zero valid offers.");
    }
    return { source: "vast-live", offers };
  }

  return { source: "vast-mock", offers: mockOffers };
}

function getSourceQueryProfile(mode: string): Prisma.InputJsonValue {
  if (mode === "vast") {
    return {
      endpoint: process.env.VAST_API_URL ?? "https://console.vast.ai/api/v0/bundles/",
      method: (process.env.VAST_API_METHOD ?? "POST").toUpperCase(),
      activeLeasesEndpoint: process.env.VAST_ACTIVE_LEASES_URL ?? null,
      activeLeasesMethod: (process.env.VAST_ACTIVE_LEASES_METHOD ?? "GET").toUpperCase(),
      requestJson:
        process.env.VAST_REQUEST_JSON ??
        JSON.stringify({
          limit: 5000,
          type: "on-demand",
          verified: { eq: true },
          rentable: { eq: true },
          rented: { eq: false },
        }),
    } as Prisma.InputJsonValue;
  }

  return {
    mode: "mock",
    preset: "gpu-market-dashboard-seed-v2",
  } as Prisma.InputJsonValue;
}

function stableSignatureForCollisionCheck(offer: SnapshotOfferInput): string {
  return [
    offer.offerId,
    offer.machineId ?? "mna",
    offer.hostId ?? "hna",
    offer.gpuName,
    offer.numGpus,
    offer.offerType ?? "unknown",
    offer.gpuRamGb ?? "grna",
    offer.cpuCores ?? "ccna",
    offer.ramGb ?? "rgna",
  ].join("::");
}

async function main() {
  const mode = (process.env.INGEST_MODE ?? "mock").toLowerCase();
  const { source, offers } = await resolveOffers();
  const sourceQuery = getSourceQueryProfile(mode);
  const sourceQueryHash = createHash("sha256")
    .update(JSON.stringify(sourceQuery))
    .digest("hex");
  const capturedAt = new Date();

  const preparedOffers = offers.map((offer) => {
    const identity = buildOfferIdentity({
      source,
      offerExternalId: offer.offerId,
      machineId: offer.machineId,
      hostId: offer.hostId,
      gpuName: offer.gpuName,
      numGpus: offer.numGpus,
      offerType: offer.offerType,
      gpuRamGb: offer.gpuRamGb,
      cpuCores: offer.cpuCores,
      ramGb: offer.ramGb,
      diskGb: offer.diskGb,
      reliabilityScore: offer.reliabilityScore,
      verified: offer.verified,
      pricePerHour: offer.pricePerHour,
      inetDownMbps: offer.inetDownMbps,
      inetUpMbps: offer.inetUpMbps,
      geolocation: offer.geolocation,
      sourceFingerprint:
        offer.hostId == null || offer.machineId == null ? null : `${offer.hostId}:${offer.machineId}`,
    });

    return {
      ...offer,
      source,
      capturedAt,
      offerExternalId: identity.offerExternalId,
      offerFingerprint: identity.fingerprint,
      stableOfferFingerprint: identity.stableOfferFingerprint,
      versionFingerprint: identity.versionFingerprint,
      identityStrategy: identity.strategy,
      identityQualityScore: identity.identityQualityScore,
      offerType: offer.offerType?.trim().toLowerCase() || "unknown",
      rawJson: {
        ...(offer.rawJson as Record<string, unknown>),
        ingestIdentityStrategy: identity.strategy,
        ingestIdentityQualityScore: identity.identityQualityScore,
      } as Prisma.InputJsonValue,
    };
  });

  const collisions = detectFingerprintCollisions(
    preparedOffers.map((offer) => ({
      fingerprint: offer.offerFingerprint,
      signature: stableSignatureForCollisionCheck(offer),
    })),
  );

  if (collisions.size > 0) {
    console.warn(`[collect] fingerprint collisions detected (${collisions.size}).`);
    for (const [fingerprint, signatures] of collisions.entries()) {
      console.warn(`[collect] collision ${fingerprint}: ${Array.from(signatures).join(" | ")}`);
    }
  }

  const snapshot = await prisma.marketSnapshot.create({
    data: {
      source,
      ingestMode: mode,
      sourceQueryHash,
      sourceQuery,
      capturedAt,
      offers: {
        create: preparedOffers.map((offer) => ({
          source: offer.source,
          capturedAt: offer.capturedAt,
          offerId: offer.offerId,
          offerExternalId: offer.offerExternalId,
          offerFingerprint: offer.offerFingerprint,
          stableOfferFingerprint: offer.stableOfferFingerprint,
          versionFingerprint: offer.versionFingerprint,
          identityStrategy: offer.identityStrategy,
          identityQualityScore: offer.identityQualityScore,
          hostId: offer.hostId,
          machineId: offer.machineId,
          gpuName: offer.gpuName,
          numGpus: offer.numGpus,
          offerType: offer.offerType,
          gpuRamGb: offer.gpuRamGb,
          cpuCores: offer.cpuCores,
          ramGb: offer.ramGb,
          diskGb: offer.diskGb,
          inetUpMbps: offer.inetUpMbps,
          inetDownMbps: offer.inetDownMbps,
          rentable: offer.rentable,
          rented: offer.rented,
          verified: offer.verified,
          reliabilityScore: offer.reliabilityScore,
          pricePerHour: offer.pricePerHour,
          dlperf: offer.dlperf,
          timeRemaining: offer.timeRemaining,
          geolocation: offer.geolocation,
          rawJson: offer.rawJson,
        })),
      },
    },
    include: {
      offers: true,
    },
  });

  console.log(
    `Created ${source} snapshot ${snapshot.id} with ${snapshot.offers.length} offers at ${capturedAt.toISOString()}.`,
  );
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

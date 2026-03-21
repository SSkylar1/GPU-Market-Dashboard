import "dotenv/config";
import { createHash } from "node:crypto";
import { PrismaClient, type Prisma } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { collectVastOffers } from "../src/lib/vast/collector";

type SnapshotOfferInput = {
  offerId: string;
  gpuName: string;
  numGpus: number;
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
  rawJson: Prisma.InputJsonValue;
};

const mockOffers: SnapshotOfferInput[] = [
  {
    offerId: "vast-mock-rtx4000ada-1",
    gpuName: "RTX 4000 Ada",
    numGpus: 1,
    gpuRamGb: 20,
    rentable: true,
    rented: false,
    pricePerHour: 0.62,
    rawJson: { provider: "mock", city: "Denver", verified: true },
  },
  {
    offerId: "vast-mock-rtx4000ada-2",
    gpuName: "RTX 4000 Ada",
    numGpus: 1,
    gpuRamGb: 20,
    rentable: false,
    rented: true,
    pricePerHour: 0.69,
    rawJson: { provider: "mock", city: "Austin", verified: true },
  },
  {
    offerId: "vast-mock-l4-1",
    gpuName: "NVIDIA L4",
    numGpus: 1,
    gpuRamGb: 24,
    rentable: true,
    rented: false,
    pricePerHour: 0.74,
    rawJson: { provider: "mock", city: "Phoenix", verified: false },
  },
  {
    offerId: "vast-mock-l4-2",
    gpuName: "NVIDIA L4",
    numGpus: 1,
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
      requestJson:
        process.env.VAST_REQUEST_JSON ??
        JSON.stringify({
          limit: 100,
          type: "on-demand",
          verified: { eq: true },
          rentable: { eq: true },
          rented: { eq: false },
        }),
    } as Prisma.InputJsonValue;
  }

  return {
    mode: "mock",
    preset: "gpu-market-dashboard-seed-v1",
  } as Prisma.InputJsonValue;
}

async function main() {
  const mode = (process.env.INGEST_MODE ?? "mock").toLowerCase();
  const { source, offers } = await resolveOffers();
  const sourceQuery = getSourceQueryProfile(mode);
  const sourceQueryHash = createHash("sha256")
    .update(JSON.stringify(sourceQuery))
    .digest("hex");

  const snapshot = await prisma.marketSnapshot.create({
    data: {
      source,
      ingestMode: mode,
      sourceQueryHash,
      sourceQuery,
      capturedAt: new Date(),
      offers: {
        create: offers.map((offer) => ({
          offerId: offer.offerId,
          gpuName: offer.gpuName,
          numGpus: offer.numGpus,
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
          rawJson: offer.rawJson,
        })),
      },
    },
    include: {
      offers: true,
    },
  });

  console.log(
    `Created ${source} snapshot ${snapshot.id} with ${snapshot.offers.length} offers.`,
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

import "dotenv/config";
import { PrismaClient, type Offer } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { floorToUtcHalfHour, percentile, summarizeOffers } from "../src/lib/metrics/aggregation";

type TrendAccumulator = {
  snapshotIds: Set<string>;
  source: string;
  gpuName: string;
  bucketStartUtc: Date;
  latestSnapshotId: string;
  latestCapturedAt: Date;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  prices: number[];
};

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

async function upsertLatestSnapshotRollups() {
  const snapshot = await prisma.marketSnapshot.findFirst({
    orderBy: { capturedAt: "desc" },
    include: { offers: true },
  });

  if (!snapshot) {
    console.log("No snapshots found. Run npm run collect first.");
    return null;
  }

  const grouped = new Map<string, Offer[]>();
  for (const offer of snapshot.offers) {
    const key = offer.gpuName;
    const current = grouped.get(key) ?? [];
    current.push(offer);
    grouped.set(key, current);
  }

  for (const [gpuName, offers] of grouped.entries()) {
    const rollup = summarizeOffers(offers);

    await prisma.gpuRollup.upsert({
      where: {
        snapshotId_gpuName: {
          snapshotId: snapshot.id,
          gpuName,
        },
      },
      create: {
        snapshotId: snapshot.id,
        gpuName,
        totalOffers: rollup.totalOffers,
        rentableOffers: rollup.rentableOffers,
        rentedOffers: rollup.rentedOffers,
        impliedUtilization: rollup.impliedUtilization,
        minPrice: rollup.minPrice,
        medianPrice: rollup.medianPrice,
        p90Price: rollup.p90Price,
      },
      update: {
        totalOffers: rollup.totalOffers,
        rentableOffers: rollup.rentableOffers,
        rentedOffers: rollup.rentedOffers,
        impliedUtilization: rollup.impliedUtilization,
        minPrice: rollup.minPrice,
        medianPrice: rollup.medianPrice,
        p90Price: rollup.p90Price,
      },
    });
  }

  return {
    snapshotId: snapshot.id,
    gpuCount: grouped.size,
  };
}

async function upsertTrendAggregates() {
  const snapshots = await prisma.marketSnapshot.findMany({
    orderBy: { capturedAt: "asc" },
    include: { offers: true },
  });

  const grouped = new Map<string, TrendAccumulator>();

  for (const snapshot of snapshots) {
    const bucketStartUtc = floorToUtcHalfHour(snapshot.capturedAt);

    for (const offer of snapshot.offers) {
      const key = `${snapshot.source}::${offer.gpuName}::${bucketStartUtc.toISOString()}`;
      const existing = grouped.get(key);

      if (!existing) {
        grouped.set(key, {
          snapshotIds: new Set([snapshot.id]),
          source: snapshot.source,
          gpuName: offer.gpuName,
          bucketStartUtc,
          latestSnapshotId: snapshot.id,
          latestCapturedAt: snapshot.capturedAt,
          totalOffers: 1,
          rentableOffers: offer.rentable ? 1 : 0,
          rentedOffers: offer.rented ? 1 : 0,
          prices: offer.pricePerHour == null ? [] : [offer.pricePerHour],
        });
        continue;
      }

      existing.snapshotIds.add(snapshot.id);
      existing.totalOffers += 1;
      existing.rentableOffers += offer.rentable ? 1 : 0;
      existing.rentedOffers += offer.rented ? 1 : 0;
      if (offer.pricePerHour != null) {
        existing.prices.push(offer.pricePerHour);
      }

      if (snapshot.capturedAt > existing.latestCapturedAt) {
        existing.latestCapturedAt = snapshot.capturedAt;
        existing.latestSnapshotId = snapshot.id;
      }
    }
  }

  for (const aggregate of grouped.values()) {
    const prices = [...aggregate.prices].sort((a, b) => a - b);
    const minPrice = prices.length > 0 ? prices[0] : null;
    const medianPrice = percentile(prices, 0.5);
    const p90Price = percentile(prices, 0.9);
    const impliedUtilization =
      aggregate.totalOffers === 0 ? 0 : 1 - aggregate.rentableOffers / aggregate.totalOffers;
    const availabilityRatio =
      aggregate.totalOffers === 0 ? 0 : aggregate.rentableOffers / aggregate.totalOffers;

    await prisma.gpuTrendAggregate.upsert({
      where: {
        gpuName_bucketStartUtc_source: {
          gpuName: aggregate.gpuName,
          bucketStartUtc: aggregate.bucketStartUtc,
          source: aggregate.source,
        },
      },
      create: {
        snapshotId: aggregate.latestSnapshotId,
        source: aggregate.source,
        gpuName: aggregate.gpuName,
        bucketStartUtc: aggregate.bucketStartUtc,
        snapshotCount: aggregate.snapshotIds.size,
        totalOffers: aggregate.totalOffers,
        rentableOffers: aggregate.rentableOffers,
        rentedOffers: aggregate.rentedOffers,
        impliedUtilization,
        availabilityRatio,
        minPrice,
        medianPrice,
        p90Price,
      },
      update: {
        snapshotId: aggregate.latestSnapshotId,
        snapshotCount: aggregate.snapshotIds.size,
        totalOffers: aggregate.totalOffers,
        rentableOffers: aggregate.rentableOffers,
        rentedOffers: aggregate.rentedOffers,
        impliedUtilization,
        availabilityRatio,
        minPrice,
        medianPrice,
        p90Price,
      },
    });
  }

  return grouped.size;
}

async function main() {
  const latestRollupSummary = await upsertLatestSnapshotRollups();
  if (!latestRollupSummary) {
    return;
  }

  const aggregateCount = await upsertTrendAggregates();

  console.log(
    `Computed ${latestRollupSummary.gpuCount} latest rollups from snapshot ${latestRollupSummary.snapshotId}.`,
  );
  console.log(`Upserted ${aggregateCount} UTC half-hour trend aggregate rows.`);
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

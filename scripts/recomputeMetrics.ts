import "dotenv/config";
import { PrismaClient, type Offer } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { floorToUtcHalfHour, percentile, summarizeOffers } from "../src/lib/metrics/aggregation";
import { deriveMarketPressureFromPair, safeDiv } from "../src/lib/metrics/intelligence";

type TrendAccumulator = {
  snapshotIds: Set<string>;
  source: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  bucketStartUtc: Date;
  latestSnapshotId: string;
  latestCapturedAt: Date;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  prices: number[];
};

type ChurnSegmentAccumulator = {
  source: string;
  sourceQueryHash: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  bucketStartUtc: Date;
  snapshotId: string;
  previousSnapshotId: string;
  newOfferCount: number;
  disappearedOfferCount: number;
};

type TrendEnrichmentAccumulator = {
  newOfferCount: number;
  disappearedOfferCount: number;
  netSupplyChange: number;
  priorVisibleSupplySum: number;
  medianPriceChangeWeightedSum: number;
  medianPriceChangeWeight: number;
  rentableShareChangeWeightedSum: number;
  rentableShareChangeWeight: number;
  pressureScoreWeightedSum: number;
  pressureChurnWeightedSum: number;
  pressureSupplyWeightedSum: number;
  pressurePriceWeightedSum: number;
  pressureAvailabilityWeightedSum: number;
  pressureWeight: number;
  lowBandDisappearedCount: number;
  midBandDisappearedCount: number;
  highBandDisappearedCount: number;
  lowBandPriorCount: number;
  midBandPriorCount: number;
  highBandPriorCount: number;
};

type TrendEnrichment = {
  newOfferCount: number;
  disappearedOfferCount: number;
  newOfferRate: number;
  disappearedRate: number;
  netSupplyChange: number;
  medianPriceChange: number | null;
  rentableShareChange: number | null;
  marketPressureScore: number | null;
  marketPressureChurnComponent: number | null;
  marketPressureSupplyComponent: number | null;
  marketPressurePriceComponent: number | null;
  marketPressureAvailabilityComponent: number | null;
  lowBandDisappearedCount: number;
  midBandDisappearedCount: number;
  highBandDisappearedCount: number;
  lowBandDisappearedRate: number;
  midBandDisappearedRate: number;
  highBandDisappearedRate: number;
};

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

function normalizeOfferType(offerType: string | null): string {
  return offerType?.trim().toLowerCase() || "unknown";
}

function segmentKey(offer: Offer): string {
  return `${offer.gpuName}::${offer.numGpus}::${normalizeOfferType(offer.offerType)}`;
}

function trendKey(input: {
  source: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  bucketStartUtc: Date;
}) {
  return `${input.source}::${input.gpuName}::${input.numGpus}::${input.offerType}::${input.bucketStartUtc.toISOString()}`;
}

/**
 * Identity priority:
 * 1) offerId when sourced from provider (non-fallback)
 * 2) machineId + gpuName + numGpus + pricePerHour
 * 3) weak fallback: row primary key (documented weak identity)
 */
function offerIdentity(offer: Offer): string {
  if (offer.offerId && !offer.offerId.startsWith("fallback:")) {
    return `offer:${offer.offerId}`;
  }

  if (offer.machineId != null && offer.pricePerHour != null) {
    return `machine:${offer.machineId}:${offer.gpuName}:${offer.numGpus}:${offer.pricePerHour.toFixed(6)}`;
  }

  return `weak:${offer.id}`;
}

function classifyPriceBand(price: number, p10: number, p90: number): "low" | "mid" | "high" {
  if (price <= p10) return "low";
  if (price >= p90) return "high";
  return "mid";
}

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
    const key = segmentKey(offer);
    const current = grouped.get(key) ?? [];
    current.push(offer);
    grouped.set(key, current);
  }

  for (const [segment, offers] of grouped.entries()) {
    const [gpuName, numGpusRaw, offerTypeRaw] = segment.split("::");
    const numGpus = Number(numGpusRaw);
    const offerType = offerTypeRaw || "unknown";
    const rollup = summarizeOffers(offers);

    await prisma.gpuRollup.upsert({
      where: {
        snapshotId_gpuName_numGpus_offerType: {
          snapshotId: snapshot.id,
          gpuName,
          numGpus,
          offerType,
        },
      },
      create: {
        snapshotId: snapshot.id,
        gpuName,
        numGpus,
        offerType,
        totalOffers: rollup.totalOffers,
        rentableOffers: rollup.rentableOffers,
        rentedOffers: rollup.rentedOffers,
        impliedUtilization: rollup.impliedUtilization,
        minPrice: rollup.minPrice,
        p10Price: rollup.p10Price,
        medianPrice: rollup.medianPrice,
        p90Price: rollup.p90Price,
      },
      update: {
        totalOffers: rollup.totalOffers,
        rentableOffers: rollup.rentableOffers,
        rentedOffers: rollup.rentedOffers,
        impliedUtilization: rollup.impliedUtilization,
        minPrice: rollup.minPrice,
        p10Price: rollup.p10Price,
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

async function upsertTrendAggregates(enrichmentByTrendKey: Map<string, TrendEnrichment>) {
  const snapshots = await prisma.marketSnapshot.findMany({
    orderBy: { capturedAt: "asc" },
    include: { offers: true },
  });

  const grouped = new Map<string, TrendAccumulator>();

  for (const snapshot of snapshots) {
    const bucketStartUtc = floorToUtcHalfHour(snapshot.capturedAt);

    for (const offer of snapshot.offers) {
      const offerType = normalizeOfferType(offer.offerType);
      const key = trendKey({
        source: snapshot.source,
        gpuName: offer.gpuName,
        numGpus: offer.numGpus,
        offerType,
        bucketStartUtc,
      });
      const existing = grouped.get(key);

      if (!existing) {
        grouped.set(key, {
          snapshotIds: new Set([snapshot.id]),
          source: snapshot.source,
          gpuName: offer.gpuName,
          numGpus: offer.numGpus,
          offerType,
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
    const p10Price = percentile(prices, 0.1);
    const medianPrice = percentile(prices, 0.5);
    const p90Price = percentile(prices, 0.9);
    const impliedUtilization =
      aggregate.totalOffers === 0 ? 0 : 1 - aggregate.rentableOffers / aggregate.totalOffers;
    const availabilityRatio =
      aggregate.totalOffers === 0 ? 0 : aggregate.rentableOffers / aggregate.totalOffers;

    const enriched =
      enrichmentByTrendKey.get(
        trendKey({
          source: aggregate.source,
          gpuName: aggregate.gpuName,
          numGpus: aggregate.numGpus,
          offerType: aggregate.offerType,
          bucketStartUtc: aggregate.bucketStartUtc,
        }),
      ) ?? null;

    await prisma.gpuTrendAggregate.upsert({
      where: {
        gpuName_numGpus_offerType_bucketStartUtc_source: {
          gpuName: aggregate.gpuName,
          numGpus: aggregate.numGpus,
          offerType: aggregate.offerType,
          bucketStartUtc: aggregate.bucketStartUtc,
          source: aggregate.source,
        },
      },
      create: {
        snapshotId: aggregate.latestSnapshotId,
        source: aggregate.source,
        gpuName: aggregate.gpuName,
        numGpus: aggregate.numGpus,
        offerType: aggregate.offerType,
        bucketStartUtc: aggregate.bucketStartUtc,
        snapshotCount: aggregate.snapshotIds.size,
        totalOffers: aggregate.totalOffers,
        rentableOffers: aggregate.rentableOffers,
        rentedOffers: aggregate.rentedOffers,
        impliedUtilization,
        availabilityRatio,
        minPrice,
        p10Price,
        medianPrice,
        p90Price,
        newOfferCount: enriched?.newOfferCount ?? null,
        disappearedOfferCount: enriched?.disappearedOfferCount ?? null,
        newOfferRate: enriched?.newOfferRate ?? null,
        disappearedRate: enriched?.disappearedRate ?? null,
        netSupplyChange: enriched?.netSupplyChange ?? null,
        medianPriceChange: enriched?.medianPriceChange ?? null,
        rentableShareChange: enriched?.rentableShareChange ?? null,
        marketPressureScore: enriched?.marketPressureScore ?? null,
        marketPressureChurnComponent: enriched?.marketPressureChurnComponent ?? null,
        marketPressureSupplyComponent: enriched?.marketPressureSupplyComponent ?? null,
        marketPressurePriceComponent: enriched?.marketPressurePriceComponent ?? null,
        marketPressureAvailabilityComponent: enriched?.marketPressureAvailabilityComponent ?? null,
        lowBandDisappearedCount: enriched?.lowBandDisappearedCount ?? null,
        midBandDisappearedCount: enriched?.midBandDisappearedCount ?? null,
        highBandDisappearedCount: enriched?.highBandDisappearedCount ?? null,
        lowBandDisappearedRate: enriched?.lowBandDisappearedRate ?? null,
        midBandDisappearedRate: enriched?.midBandDisappearedRate ?? null,
        highBandDisappearedRate: enriched?.highBandDisappearedRate ?? null,
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
        p10Price,
        medianPrice,
        p90Price,
        newOfferCount: enriched?.newOfferCount ?? null,
        disappearedOfferCount: enriched?.disappearedOfferCount ?? null,
        newOfferRate: enriched?.newOfferRate ?? null,
        disappearedRate: enriched?.disappearedRate ?? null,
        netSupplyChange: enriched?.netSupplyChange ?? null,
        medianPriceChange: enriched?.medianPriceChange ?? null,
        rentableShareChange: enriched?.rentableShareChange ?? null,
        marketPressureScore: enriched?.marketPressureScore ?? null,
        marketPressureChurnComponent: enriched?.marketPressureChurnComponent ?? null,
        marketPressureSupplyComponent: enriched?.marketPressureSupplyComponent ?? null,
        marketPressurePriceComponent: enriched?.marketPressurePriceComponent ?? null,
        marketPressureAvailabilityComponent: enriched?.marketPressureAvailabilityComponent ?? null,
        lowBandDisappearedCount: enriched?.lowBandDisappearedCount ?? null,
        midBandDisappearedCount: enriched?.midBandDisappearedCount ?? null,
        highBandDisappearedCount: enriched?.highBandDisappearedCount ?? null,
        lowBandDisappearedRate: enriched?.lowBandDisappearedRate ?? null,
        midBandDisappearedRate: enriched?.midBandDisappearedRate ?? null,
        highBandDisappearedRate: enriched?.highBandDisappearedRate ?? null,
      },
    });
  }

  return grouped.size;
}

async function upsertChurnAggregates() {
  const snapshots = await prisma.marketSnapshot.findMany({
    orderBy: { capturedAt: "asc" },
    include: {
      offers: true,
    },
  });

  const byQueryHash = new Map<string, typeof snapshots>();
  for (const snapshot of snapshots) {
    if (!snapshot.sourceQueryHash) {
      continue;
    }
    const key = `${snapshot.source}::${snapshot.sourceQueryHash}`;
    const current = byQueryHash.get(key) ?? [];
    current.push(snapshot);
    byQueryHash.set(key, current);
  }

  const churnByBucketAndSegment = new Map<string, ChurnSegmentAccumulator>();
  const enrichmentAccumulators = new Map<string, TrendEnrichmentAccumulator>();

  for (const sequence of byQueryHash.values()) {
    for (let i = 1; i < sequence.length; i += 1) {
      const previousSnapshot = sequence[i - 1];
      const currentSnapshot = sequence[i];
      const bucketStartUtc = floorToUtcHalfHour(currentSnapshot.capturedAt);

      const previousOffersBySegment = new Map<string, Offer[]>();
      for (const offer of previousSnapshot.offers) {
        const key = segmentKey(offer);
        const current = previousOffersBySegment.get(key) ?? [];
        current.push(offer);
        previousOffersBySegment.set(key, current);
      }

      const currentOffersBySegment = new Map<string, Offer[]>();
      for (const offer of currentSnapshot.offers) {
        const key = segmentKey(offer);
        const current = currentOffersBySegment.get(key) ?? [];
        current.push(offer);
        currentOffersBySegment.set(key, current);
      }

      const allSegments = new Set<string>([
        ...previousOffersBySegment.keys(),
        ...currentOffersBySegment.keys(),
      ]);

      for (const segment of allSegments) {
        const [gpuName, numGpusRaw, offerTypeRaw] = segment.split("::");
        const numGpus = Number(numGpusRaw);
        const offerType = offerTypeRaw || "unknown";
        const previousOffers = previousOffersBySegment.get(segment) ?? [];
        const currentOffers = currentOffersBySegment.get(segment) ?? [];

        const previousIdentitySet = new Set<string>();
        const previousByIdentity = new Map<string, Offer>();
        for (const offer of previousOffers) {
          const identity = offerIdentity(offer);
          previousIdentitySet.add(identity);
          if (!previousByIdentity.has(identity)) {
            previousByIdentity.set(identity, offer);
          }
        }

        const currentIdentitySet = new Set<string>();
        for (const offer of currentOffers) {
          currentIdentitySet.add(offerIdentity(offer));
        }

        let newOfferCount = 0;
        for (const identity of currentIdentitySet.values()) {
          if (!previousIdentitySet.has(identity)) {
            newOfferCount += 1;
          }
        }

        let disappearedOfferCount = 0;
        const disappearedOffers: Offer[] = [];
        for (const identity of previousIdentitySet.values()) {
          if (!currentIdentitySet.has(identity)) {
            disappearedOfferCount += 1;
            const offer = previousByIdentity.get(identity);
            if (offer) {
              disappearedOffers.push(offer);
            }
          }
        }

        const priorVisibleSupplyCount = previousOffers.length;
        const previousSummary = summarizeOffers(previousOffers);
        const currentSummary = summarizeOffers(currentOffers);

        const newOfferRate = safeDiv(newOfferCount, Math.max(priorVisibleSupplyCount, 1));
        const disappearedRate = safeDiv(
          disappearedOfferCount,
          Math.max(priorVisibleSupplyCount, 1),
        );
        const netSupplyChange = newOfferCount - disappearedOfferCount;
        const medianPriceChange =
          currentSummary.medianPrice == null || previousSummary.medianPrice == null
            ? null
            : currentSummary.medianPrice - previousSummary.medianPrice;
        const rentableShareChange =
          currentSummary.availabilityRatio - previousSummary.availabilityRatio;

        const pressure = deriveMarketPressureFromPair({
          newOfferRate,
          disappearedRate,
          medianPriceChange: medianPriceChange ?? 0,
          priorMedianPrice: previousSummary.medianPrice,
          rentableShareChange,
        });

        const previousPrices = previousSummary.prices;
        const p10 = percentile(previousPrices, 0.1);
        const p90 = percentile(previousPrices, 0.9);
        let lowBandPriorCount = 0;
        let midBandPriorCount = 0;
        let highBandPriorCount = 0;
        let lowBandDisappearedCount = 0;
        let midBandDisappearedCount = 0;
        let highBandDisappearedCount = 0;

        if (p10 != null && p90 != null) {
          for (const offer of previousOffers) {
            if (offer.pricePerHour == null) continue;
            const band = classifyPriceBand(offer.pricePerHour, p10, p90);
            if (band === "low") lowBandPriorCount += 1;
            if (band === "mid") midBandPriorCount += 1;
            if (band === "high") highBandPriorCount += 1;
          }

          for (const offer of disappearedOffers) {
            if (offer.pricePerHour == null) continue;
            const band = classifyPriceBand(offer.pricePerHour, p10, p90);
            if (band === "low") lowBandDisappearedCount += 1;
            if (band === "mid") midBandDisappearedCount += 1;
            if (band === "high") highBandDisappearedCount += 1;
          }
        }

        if (newOfferCount > 0 || disappearedOfferCount > 0) {
          const churnKey = `${currentSnapshot.source}::${currentSnapshot.sourceQueryHash}::${gpuName}::${numGpus}::${offerType}::${bucketStartUtc.toISOString()}`;
          const existingChurn = churnByBucketAndSegment.get(churnKey);
          if (!existingChurn) {
            churnByBucketAndSegment.set(churnKey, {
              source: currentSnapshot.source,
              sourceQueryHash: currentSnapshot.sourceQueryHash as string,
              gpuName,
              numGpus,
              offerType,
              bucketStartUtc,
              snapshotId: currentSnapshot.id,
              previousSnapshotId: previousSnapshot.id,
              newOfferCount,
              disappearedOfferCount,
            });
          } else {
            existingChurn.newOfferCount += newOfferCount;
            existingChurn.disappearedOfferCount += disappearedOfferCount;
            existingChurn.snapshotId = currentSnapshot.id;
            existingChurn.previousSnapshotId = previousSnapshot.id;
          }
        }

        const tKey = trendKey({
          source: currentSnapshot.source,
          gpuName,
          numGpus,
          offerType,
          bucketStartUtc,
        });

        const existingTrend =
          enrichmentAccumulators.get(tKey) ?? {
            newOfferCount: 0,
            disappearedOfferCount: 0,
            netSupplyChange: 0,
            priorVisibleSupplySum: 0,
            medianPriceChangeWeightedSum: 0,
            medianPriceChangeWeight: 0,
            rentableShareChangeWeightedSum: 0,
            rentableShareChangeWeight: 0,
            pressureScoreWeightedSum: 0,
            pressureChurnWeightedSum: 0,
            pressureSupplyWeightedSum: 0,
            pressurePriceWeightedSum: 0,
            pressureAvailabilityWeightedSum: 0,
            pressureWeight: 0,
            lowBandDisappearedCount: 0,
            midBandDisappearedCount: 0,
            highBandDisappearedCount: 0,
            lowBandPriorCount: 0,
            midBandPriorCount: 0,
            highBandPriorCount: 0,
          };

        const weight = Math.max(priorVisibleSupplyCount, 1);
        existingTrend.newOfferCount += newOfferCount;
        existingTrend.disappearedOfferCount += disappearedOfferCount;
        existingTrend.netSupplyChange += netSupplyChange;
        existingTrend.priorVisibleSupplySum += priorVisibleSupplyCount;
        if (medianPriceChange != null) {
          existingTrend.medianPriceChangeWeightedSum += medianPriceChange * weight;
          existingTrend.medianPriceChangeWeight += weight;
        }
        existingTrend.rentableShareChangeWeightedSum += rentableShareChange * weight;
        existingTrend.rentableShareChangeWeight += weight;
        existingTrend.pressureScoreWeightedSum += pressure.marketPressureScore * weight;
        existingTrend.pressureChurnWeightedSum += pressure.marketPressureChurnComponent * weight;
        existingTrend.pressureSupplyWeightedSum += pressure.marketPressureSupplyComponent * weight;
        existingTrend.pressurePriceWeightedSum += pressure.marketPressurePriceComponent * weight;
        existingTrend.pressureAvailabilityWeightedSum +=
          pressure.marketPressureAvailabilityComponent * weight;
        existingTrend.pressureWeight += weight;

        existingTrend.lowBandDisappearedCount += lowBandDisappearedCount;
        existingTrend.midBandDisappearedCount += midBandDisappearedCount;
        existingTrend.highBandDisappearedCount += highBandDisappearedCount;
        existingTrend.lowBandPriorCount += lowBandPriorCount;
        existingTrend.midBandPriorCount += midBandPriorCount;
        existingTrend.highBandPriorCount += highBandPriorCount;

        enrichmentAccumulators.set(tKey, existingTrend);
      }
    }
  }

  for (const churn of churnByBucketAndSegment.values()) {
    await prisma.gpuChurnAggregate.upsert({
      where: {
        source_gpuName_numGpus_offerType_bucketStartUtc: {
          source: churn.source,
          gpuName: churn.gpuName,
          numGpus: churn.numGpus,
          offerType: churn.offerType,
          bucketStartUtc: churn.bucketStartUtc,
        },
      },
      create: {
        snapshotId: churn.snapshotId,
        previousSnapshotId: churn.previousSnapshotId,
        source: churn.source,
        sourceQueryHash: churn.sourceQueryHash,
        gpuName: churn.gpuName,
        numGpus: churn.numGpus,
        offerType: churn.offerType,
        bucketStartUtc: churn.bucketStartUtc,
        newOfferCount: churn.newOfferCount,
        disappearedOfferCount: churn.disappearedOfferCount,
      },
      update: {
        snapshotId: churn.snapshotId,
        previousSnapshotId: churn.previousSnapshotId,
        sourceQueryHash: churn.sourceQueryHash,
        newOfferCount: churn.newOfferCount,
        disappearedOfferCount: churn.disappearedOfferCount,
      },
    });
  }

  const enrichmentByTrendKey = new Map<string, TrendEnrichment>();
  for (const [key, value] of enrichmentAccumulators.entries()) {
    enrichmentByTrendKey.set(key, {
      newOfferCount: value.newOfferCount,
      disappearedOfferCount: value.disappearedOfferCount,
      newOfferRate: safeDiv(value.newOfferCount, Math.max(value.priorVisibleSupplySum, 1)),
      disappearedRate: safeDiv(
        value.disappearedOfferCount,
        Math.max(value.priorVisibleSupplySum, 1),
      ),
      netSupplyChange: value.netSupplyChange,
      medianPriceChange:
        value.medianPriceChangeWeight === 0
          ? null
          : value.medianPriceChangeWeightedSum / value.medianPriceChangeWeight,
      rentableShareChange:
        value.rentableShareChangeWeight === 0
          ? null
          : value.rentableShareChangeWeightedSum / value.rentableShareChangeWeight,
      marketPressureScore:
        value.pressureWeight === 0 ? null : value.pressureScoreWeightedSum / value.pressureWeight,
      marketPressureChurnComponent:
        value.pressureWeight === 0 ? null : value.pressureChurnWeightedSum / value.pressureWeight,
      marketPressureSupplyComponent:
        value.pressureWeight === 0 ? null : value.pressureSupplyWeightedSum / value.pressureWeight,
      marketPressurePriceComponent:
        value.pressureWeight === 0 ? null : value.pressurePriceWeightedSum / value.pressureWeight,
      marketPressureAvailabilityComponent:
        value.pressureWeight === 0
          ? null
          : value.pressureAvailabilityWeightedSum / value.pressureWeight,
      lowBandDisappearedCount: value.lowBandDisappearedCount,
      midBandDisappearedCount: value.midBandDisappearedCount,
      highBandDisappearedCount: value.highBandDisappearedCount,
      lowBandDisappearedRate: safeDiv(
        value.lowBandDisappearedCount,
        Math.max(value.lowBandPriorCount, 1),
      ),
      midBandDisappearedRate: safeDiv(
        value.midBandDisappearedCount,
        Math.max(value.midBandPriorCount, 1),
      ),
      highBandDisappearedRate: safeDiv(
        value.highBandDisappearedCount,
        Math.max(value.highBandPriorCount, 1),
      ),
    });
  }

  return {
    churnAggregateCount: churnByBucketAndSegment.size,
    enrichmentByTrendKey,
  };
}

async function main() {
  const latestRollupSummary = await upsertLatestSnapshotRollups();
  if (!latestRollupSummary) {
    return;
  }

  const churnResult = await upsertChurnAggregates();
  const aggregateCount = await upsertTrendAggregates(churnResult.enrichmentByTrendKey);

  console.log(
    `Computed ${latestRollupSummary.gpuCount} latest rollups from snapshot ${latestRollupSummary.snapshotId}.`,
  );
  console.log(`Upserted ${aggregateCount} UTC half-hour trend aggregate rows.`);
  console.log(`Upserted ${churnResult.churnAggregateCount} UTC half-hour churn aggregate rows.`);
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

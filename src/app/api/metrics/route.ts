import { subDays } from "date-fns";
import { prisma } from "@/lib/db/prisma";
import {
  buildRecommendation,
  classifyMarketRegime,
  computeCompetitionMetrics,
  computeWindowTrendSummary,
  estimateExpectedUtilization,
  estimateRoiContext,
} from "@/lib/metrics/intelligence";
import { floorToUtcHalfHour } from "@/lib/metrics/aggregation";
import { estimatePowerAndCost } from "@/lib/scoring/hardwareDefaults";

type TrendRow = {
  source: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  bucketStartUtc: Date;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  minPrice: number | null;
  p10Price: number | null;
  medianPrice: number | null;
  p90Price: number | null;
  newOffers: number | null;
  disappearedOffers: number | null;
  newOfferRate: number | null;
  disappearedRate: number | null;
  cohortPressureScore: number | null;
  medianPriceChange: number | null;
  lowBandDisappearedRate: number | null;
  midBandDisappearedRate: number | null;
  highBandDisappearedRate: number | null;
};

function cohortKey(gpuName: string, numGpus: number, offerType: string) {
  return `${gpuName}::${numGpus}::${offerType || "unknown"}`;
}

export async function GET() {
  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findFirst: (args: { orderBy: { bucketStartUtc: "desc" }; select: { bucketStartUtc: true } }) => Promise<{ bucketStartUtc: Date } | null>;
      findMany: (args: {
        where: { source: string; bucketStartUtc: { gte: Date } };
        select: {
          source: true;
          gpuName: true;
          numGpus: true;
          offerType: true;
          bucketStartUtc: true;
          totalOffers: true;
          rentableOffers: true;
          rentedOffers: true;
          minPrice: true;
          p10Price: true;
          medianPrice: true;
          p90Price: true;
          newOffers: true;
          disappearedOffers: true;
          newOfferRate: true;
          disappearedRate: true;
          cohortPressureScore: true;
          medianPriceChange: true;
          lowBandDisappearedRate: true;
          midBandDisappearedRate: true;
          highBandDisappearedRate: true;
        };
      }) => Promise<TrendRow[]>;
    };
  }).gpuTrendAggregate;

  const [latestSnapshot, latestAggregateBucket] = await Promise.all([
    prisma.marketSnapshot.findFirst({
      orderBy: { capturedAt: "desc" },
      include: {
        offers: {
          select: {
            gpuName: true,
            numGpus: true,
            offerType: true,
            hostId: true,
            machineId: true,
            reliabilityScore: true,
          },
        },
        rollups: {
          orderBy: { totalOffers: "desc" },
          take: 100,
        },
      },
    }),
    trendClient
      ? trendClient.findFirst({
          orderBy: { bucketStartUtc: "desc" },
          select: { bucketStartUtc: true },
        })
      : Promise.resolve(null),
  ]);

  const previousSnapshot =
    latestSnapshot?.sourceQueryHash == null
      ? null
      : await prisma.marketSnapshot.findFirst({
          where: { sourceQueryHash: latestSnapshot.sourceQueryHash, id: { not: latestSnapshot.id } },
          orderBy: { capturedAt: "desc" },
          include: { rollups: true },
        });

  const previousMap = new Map(
    (previousSnapshot?.rollups ?? []).map((rollup) => [cohortKey(rollup.gpuName, rollup.numGpus, rollup.offerType), rollup]),
  );

  const sourceQueryRecord =
    latestSnapshot?.sourceQuery && typeof latestSnapshot.sourceQuery === "object"
      ? (latestSnapshot.sourceQuery as Record<string, unknown>)
      : null;
  const hasSecondaryLeaseSignal =
    sourceQueryRecord != null &&
    typeof sourceQueryRecord.activeLeasesEndpoint === "string" &&
    sourceQueryRecord.activeLeasesEndpoint.trim().length > 0;

  const latestBucketStartUtc =
    latestAggregateBucket?.bucketStartUtc ??
    (latestSnapshot ? floorToUtcHalfHour(latestSnapshot.capturedAt) : null);

  const since7d = latestSnapshot ? subDays(latestSnapshot.capturedAt, 7) : subDays(new Date(), 7);

  const trendRows =
    trendClient && latestSnapshot
      ? await trendClient.findMany({
          where: { source: latestSnapshot.source, bucketStartUtc: { gte: since7d } },
          select: {
            source: true,
            gpuName: true,
            numGpus: true,
            offerType: true,
            bucketStartUtc: true,
            totalOffers: true,
            rentableOffers: true,
            rentedOffers: true,
            minPrice: true,
            p10Price: true,
            medianPrice: true,
            p90Price: true,
            newOffers: true,
            disappearedOffers: true,
            newOfferRate: true,
            disappearedRate: true,
            cohortPressureScore: true,
            medianPriceChange: true,
            lowBandDisappearedRate: true,
            midBandDisappearedRate: true,
            highBandDisappearedRate: true,
          },
        })
      : [];

  const trendByCohort = new Map<string, TrendRow[]>();
  for (const row of trendRows) {
    const key = cohortKey(row.gpuName, row.numGpus, row.offerType);
    const current = trendByCohort.get(key) ?? [];
    current.push(row);
    trendByCohort.set(key, current);
  }
  for (const points of trendByCohort.values()) {
    points.sort((a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime());
  }

  const offerByCohort = new Map<
    string,
    Array<{ hostId: number | null; machineId: number | null; reliabilityScore: number | null }>
  >();
  for (const offer of latestSnapshot?.offers ?? []) {
    const key = cohortKey(offer.gpuName, offer.numGpus, offer.offerType?.trim().toLowerCase() || "unknown");
    const current = offerByCohort.get(key) ?? [];
    current.push({
      hostId: offer.hostId,
      machineId: offer.machineId,
      reliabilityScore: offer.reliabilityScore,
    });
    offerByCohort.set(key, current);
  }

  const rollups = (latestSnapshot?.rollups ?? []).map((rollup) => {
    const key = cohortKey(rollup.gpuName, rollup.numGpus, rollup.offerType);
    const previous = previousMap.get(key);
    const points = trendByCohort.get(key) ?? [];
    const latestPoint = points.length === 0 ? null : points[points.length - 1];

    const trend6h = latestBucketStartUtc ? computeWindowTrendSummary(points, latestBucketStartUtc, "6h") : null;
    const trend24h = latestBucketStartUtc
      ? computeWindowTrendSummary(points, latestBucketStartUtc, "24h")
      : null;
    const trend7d = latestBucketStartUtc ? computeWindowTrendSummary(points, latestBucketStartUtc, "7d") : null;

    const competition = computeCompetitionMetrics(offerByCohort.get(key) ?? []);
    const defaults = estimatePowerAndCost(rollup.gpuName);
    const listingPrice = rollup.medianPrice ?? latestPoint?.medianPrice ?? 0;
    const expectedUtilizationEstimate = estimateExpectedUtilization({
      disappearedRate: latestPoint?.disappearedRate ?? 0,
      netSupplyChange: (latestPoint?.newOffers ?? 0) - (latestPoint?.disappearedOffers ?? 0),
      visibleSupplyCount: rollup.totalOffers,
      rentableShare: rollup.totalOffers === 0 ? 0 : rollup.rentableOffers / rollup.totalOffers,
      listingPricePerHour: listingPrice,
      medianPrice: latestPoint?.medianPrice ?? rollup.medianPrice,
      reliabilityScore: competition.avgReliabilityScore,
    });

    const roi = estimateRoiContext({
      expectedUtilizationEstimate,
      listingPricePerHour: listingPrice,
      hardwareCost: defaults.assumedHardwareCost,
      powerWatts: defaults.assumedPowerWatts,
    });

    const regime = classifyMarketRegime({
      disappearedRate: latestPoint?.disappearedRate ?? 0,
      newOfferRate: latestPoint?.newOfferRate ?? 0,
      netSupplyChange: (latestPoint?.newOffers ?? 0) - (latestPoint?.disappearedOffers ?? 0),
      medianPriceChange: latestPoint?.medianPriceChange ?? 0,
      rentableShareChange: 0,
      marketPressureScore: latestPoint?.cohortPressureScore ?? 0,
    });

    const recommendation = buildRecommendation({
      regime,
      lowBandDisappearedRate: latestPoint?.lowBandDisappearedRate ?? 0,
      midBandDisappearedRate: latestPoint?.midBandDisappearedRate ?? 0,
      highBandDisappearedRate: latestPoint?.highBandDisappearedRate ?? 0,
      topHostShare: competition.topHostShare,
      marketPressureScore: latestPoint?.cohortPressureScore ?? 0,
    });

    return {
      ...rollup,
      p10Price: latestPoint?.p10Price ?? null,
      availableShare: rollup.totalOffers === 0 ? 0 : rollup.rentableOffers / rollup.totalOffers,
      unavailableShareProxy: rollup.impliedUtilization,
      leaseSignalShare: rollup.totalOffers === 0 ? 0 : rollup.rentedOffers / rollup.totalOffers,
      leaseSignalQuality: hasSecondaryLeaseSignal ? "high" : "low",
      latestMarketPressure: latestPoint?.cohortPressureScore ?? null,
      marketPressureComponents: null,
      churn: {
        latestNewOffers: latestPoint?.newOffers ?? 0,
        latestDisappearedOffers: latestPoint?.disappearedOffers ?? 0,
        newOfferRate: latestPoint?.newOfferRate ?? 0,
        disappearedRate: latestPoint?.disappearedRate ?? 0,
        netSupplyChange: (latestPoint?.newOffers ?? 0) - (latestPoint?.disappearedOffers ?? 0),
      },
      priceBands: {
        lowBandDisappearedRate: latestPoint?.lowBandDisappearedRate ?? 0,
        midBandDisappearedRate: latestPoint?.midBandDisappearedRate ?? 0,
        highBandDisappearedRate: latestPoint?.highBandDisappearedRate ?? 0,
        lowBandDisappearedCount: 0,
        midBandDisappearedCount: 0,
        highBandDisappearedCount: 0,
      },
      competition,
      trends: {
        window6h: trend6h,
        window24h: trend24h,
        window7d: trend7d,
      },
      regime,
      recommendationLabel: recommendation.recommendationLabel,
      recommendationReasonPrimary: recommendation.recommendationReasonPrimary,
      recommendationReasonSecondary: recommendation.recommendationReasonSecondary,
      recommendationConfidenceNote: recommendation.recommendationConfidenceNote,
      roi,
      medianPriceDelta:
        previous?.medianPrice != null && rollup.medianPrice != null
          ? rollup.medianPrice - previous.medianPrice
          : null,
      unavailableShareDelta:
        previous != null ? rollup.impliedUtilization - previous.impliedUtilization : null,
    };
  });

  const freshnessMinutes =
    latestSnapshot == null ? null : Math.max(0, Math.floor((Date.now() - latestSnapshot.capturedAt.getTime()) / 60000));

  return Response.json({
    snapshotId: latestSnapshot?.id ?? null,
    capturedAt: latestSnapshot?.capturedAt ?? null,
    previousSnapshotId: previousSnapshot?.id ?? null,
    latestAggregateBucketUtc: latestAggregateBucket?.bucketStartUtc ?? null,
    freshnessMinutes,
    leaseSignalQuality: hasSecondaryLeaseSignal ? "high" : "low",
    rollups,
  });
}

import { subHours } from "date-fns";
import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { calculateScenarioScore } from "@/lib/scoring/score";
import { getRecommendedPriceBands } from "@/lib/scoring/pricing";
import {
  buildRecommendation,
  classifyMarketRegime,
  computeCompetitionMetrics,
  computeWindowTrendSummary,
  estimateExpectedUtilization,
  estimateRoiContext,
} from "@/lib/metrics/intelligence";

const requestSchema = z.object({
  gpuName: z.string().min(1),
  cohortNumGpus: z.number().int().positive().optional(),
  cohortOfferType: z.string().optional(),
  gpuCount: z.number().int().positive().max(128),
  assumedPowerWatts: z.number().int().positive().max(200000),
  assumedHardwareCost: z.number().positive(),
  electricityCostPerKwh: z.number().min(0).max(5),
  targetPaybackMonths: z.number().int().positive().max(120),
  source: z.string().optional().default("vast-live"),
  hoursWindow: z.number().int().min(6).max(168).optional().default(168),
  listingPricePerHour: z.number().positive().optional(),
});

function clamp(value: number, min = 0, max = 100): number {
  return Math.max(min, Math.min(max, value));
}

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function stddev(values: number[]): number {
  if (values.length <= 1) return 0;
  const avg = mean(values);
  const variance = mean(values.map((value) => (value - avg) ** 2));
  return Math.sqrt(variance);
}

export type MarketScoringResult = {
  overallScore: number;
  recommendation: "Buy" | "Watch" | "Avoid";
  demandScore: number;
  competitionScore: number;
  priceStrengthScore: number;
  efficiencyScore: number;
  expectedDailyRevenue: number;
  expectedDailyPowerCost: number;
  expectedDailyProfit: number;
  expectedPaybackMonths: number | null;
  confidence: {
    level: "low" | "medium" | "high";
    bucketCount: number;
    score: number;
    leaseSignalQuality: "low" | "high";
  };
  marketSignals: {
    availableShare: number;
    unavailableShareProxy: number;
    activeLeaseShare: number;
    newOfferRate: number;
    disappearedRate: number;
    netSupplyChange: number;
    marketPressureScore: number;
    leaseSignalQuality: "low" | "high";
  };
  pricing: {
    aggressive: number;
    target: number;
    premium: number;
  };
  scenarioId: string;
  scenarioScoreId: string;
  cohort: {
    numGpus: number | null;
    offerType: string | null;
  };
  regime: "tight" | "balanced" | "oversupplied";
  recommendationLabel: "Buy" | "Wait" | "Avoid" | "Race-to-bottom risk";
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
  roi: {
    expectedUtilizationEstimate: number;
    expectedDailyRevenue: number;
    estimatedDailyPowerCost: number;
    estimatedDailyMargin: number;
    paybackPeriodDays: number | null;
  };
  trends: {
    window6h: ReturnType<typeof computeWindowTrendSummary>;
    window24h: ReturnType<typeof computeWindowTrendSummary>;
    window7d: ReturnType<typeof computeWindowTrendSummary>;
  };
};

export async function scoreScenarioWithMarket(input: unknown): Promise<MarketScoringResult> {
  const parsed = requestSchema.parse(input);
  const since = subHours(new Date(), parsed.hoursWindow);

  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: {
          gpuName: string;
          numGpus?: number;
          offerType?: string;
          source: string;
          bucketStartUtc: { gte: Date };
        };
        orderBy: { bucketStartUtc: "asc" };
      }) => Promise<Array<{
        bucketStartUtc: Date;
        totalOffers: number;
        rentableOffers: number;
        rentedOffers: number;
        medianPrice: number | null;
        newOfferRate: number | null;
        disappearedRate: number | null;
        netSupplyChange: number | null;
        marketPressureScore: number | null;
        medianPriceChange: number | null;
        rentableShareChange: number | null;
        lowBandDisappearedRate: number | null;
        midBandDisappearedRate: number | null;
        highBandDisappearedRate: number | null;
      }>>;
    };
  }).gpuTrendAggregate;

  if (!trendClient) {
    throw new Error("Trend aggregates are unavailable in the current runtime. Regenerate/restart app.");
  }

  const points = await trendClient.findMany({
    where: {
      gpuName: parsed.gpuName,
      ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
      ...(parsed.cohortOfferType?.trim() ? { offerType: parsed.cohortOfferType.trim().toLowerCase() } : {}),
      source: parsed.source,
      bucketStartUtc: {
        gte: since,
      },
    },
    orderBy: {
      bucketStartUtc: "asc",
    },
  });

  if (points.length === 0) {
    throw new Error(
      `No trend points found for ${parsed.gpuName} in source ${parsed.source} within last ${parsed.hoursWindow}h.`,
    );
  }

  const latestPoint = points[points.length - 1];
  const availableSeries = points
    .filter((point) => point.totalOffers > 0)
    .map((point) => point.rentableOffers / point.totalOffers);
  const activeLeaseSeries = points
    .filter((point) => point.totalOffers > 0)
    .map((point) => point.rentedOffers / point.totalOffers);

  const avgAvailable = availableSeries.length > 0 ? mean(availableSeries) : 0;
  const avgActiveLease = activeLeaseSeries.length > 0 ? mean(activeLeaseSeries) : 0;
  const avgUnavailable = 1 - avgAvailable;

  const latestSnapshot = await prisma.marketSnapshot.findFirst({
    where: { source: parsed.source },
    orderBy: { capturedAt: "desc" },
    select: { id: true, sourceQuery: true },
  });

  const latestOffers = latestSnapshot
    ? await prisma.offer.findMany({
        where: {
          snapshotId: latestSnapshot.id,
          gpuName: parsed.gpuName,
          ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
          ...(parsed.cohortOfferType?.trim() ? { offerType: parsed.cohortOfferType.trim().toLowerCase() } : {}),
        },
        select: {
          hostId: true,
          machineId: true,
          reliabilityScore: true,
        },
      })
    : [];

  const competition = computeCompetitionMetrics(latestOffers);

  const comparablePrices = points
    .map((point) => point.medianPrice)
    .filter((value): value is number => value != null && Number.isFinite(value) && value > 0);
  const avgMedianPrice = comparablePrices.length > 0 ? mean(comparablePrices) : 0;
  const cv = avgMedianPrice > 0 ? stddev(comparablePrices) / avgMedianPrice : 1;

  const demandScore = clamp((latestPoint.marketPressureScore ?? avgUnavailable * 100));
  const competitionScore = clamp(100 - (competition.offersPerHost * 30 + competition.topHostShare * 50));
  const priceStrengthScore = clamp(100 - cv * 200);

  const listingPrice = parsed.listingPricePerHour ?? latestPoint.medianPrice ?? avgMedianPrice;
  const expectedUtilizationEstimate = estimateExpectedUtilization({
    disappearedRate: latestPoint.disappearedRate ?? 0,
    netSupplyChange: latestPoint.netSupplyChange ?? 0,
    visibleSupplyCount: latestPoint.totalOffers,
    rentableShare: latestPoint.totalOffers === 0 ? 0 : latestPoint.rentableOffers / latestPoint.totalOffers,
    listingPricePerHour: listingPrice,
    medianPrice: latestPoint.medianPrice,
    reliabilityScore: competition.avgReliabilityScore,
  });

  const roi = estimateRoiContext({
    expectedUtilizationEstimate,
    listingPricePerHour: listingPrice,
    hardwareCost: parsed.assumedHardwareCost,
    powerWatts: parsed.assumedPowerWatts,
    electricityCostPerKwh: parsed.electricityCostPerKwh,
  });

  const expectedPaybackMonths = roi.paybackPeriodDays == null ? null : roi.paybackPeriodDays / 30.4375;
  const marginPct = roi.expectedDailyRevenue > 0 ? roi.estimatedDailyMargin / roi.expectedDailyRevenue : 0;
  const paybackFit =
    expectedPaybackMonths == null
      ? 0
      : clamp((parsed.targetPaybackMonths / expectedPaybackMonths) * 100);
  const efficiencyScore = clamp(marginPct * 70 + (paybackFit / 100) * 30);

  const score = calculateScenarioScore({
    demandScore,
    priceStrengthScore,
    competitionScore,
    efficiencyScore,
  });

  const sourceQuery =
    latestSnapshot?.sourceQuery && typeof latestSnapshot.sourceQuery === "object"
      ? (latestSnapshot.sourceQuery as Record<string, unknown>)
      : null;
  const hasSecondaryLeaseSignal =
    sourceQuery != null &&
    typeof sourceQuery.activeLeasesEndpoint === "string" &&
    sourceQuery.activeLeasesEndpoint.trim().length > 0;
  const leaseSignalQuality: "low" | "high" = hasSecondaryLeaseSignal ? "high" : "low";

  const confidenceScore = clamp(
    Math.min(100, (points.length / 48) * 100) * 0.45 +
      clamp((avgAvailable + avgActiveLease) * 100) * 0.35 +
      (hasSecondaryLeaseSignal ? 100 : 35) * 0.2,
  );
  const confidenceLevel: "low" | "medium" | "high" =
    confidenceScore >= 70 ? "high" : confidenceScore >= 45 ? "medium" : "low";

  const regime = classifyMarketRegime({
    disappearedRate: latestPoint.disappearedRate ?? 0,
    newOfferRate: latestPoint.newOfferRate ?? 0,
    netSupplyChange: latestPoint.netSupplyChange ?? 0,
    medianPriceChange: latestPoint.medianPriceChange ?? 0,
    rentableShareChange: latestPoint.rentableShareChange ?? 0,
    marketPressureScore: latestPoint.marketPressureScore ?? 0,
  });

  const recommendation = buildRecommendation({
    regime,
    lowBandDisappearedRate: latestPoint.lowBandDisappearedRate ?? 0,
    midBandDisappearedRate: latestPoint.midBandDisappearedRate ?? 0,
    highBandDisappearedRate: latestPoint.highBandDisappearedRate ?? 0,
    topHostShare: competition.topHostShare,
    marketPressureScore: latestPoint.marketPressureScore ?? 0,
  });

  const pricing = getRecommendedPriceBands(comparablePrices);

  const scenario = await prisma.hardwareScenario.create({
    data: {
      gpuName: parsed.gpuName,
      gpuCount: parsed.gpuCount,
      assumedPowerWatts: parsed.assumedPowerWatts,
      assumedHardwareCost: parsed.assumedHardwareCost,
      electricityCostPerKwh: parsed.electricityCostPerKwh,
      targetPaybackMonths: parsed.targetPaybackMonths,
      notes: `source=${parsed.source};hoursWindow=${parsed.hoursWindow};buckets=${points.length};leaseSignal=${leaseSignalQuality};regime=${regime}`,
    },
  });

  const persistedScore = await prisma.scenarioScore.create({
    data: {
      scenarioId: scenario.id,
      demandScore,
      competitionScore,
      priceStrengthScore,
      efficiencyScore,
      overallScore: score.overallScore,
      recommendation: score.recommendation,
      recommendedPriceLow: pricing.aggressive,
      recommendedPriceTarget: pricing.target,
      recommendedPriceHigh: pricing.premium,
    },
  });

  return {
    overallScore: score.overallScore,
    recommendation: score.recommendation,
    demandScore: Number(demandScore.toFixed(2)),
    competitionScore: Number(competitionScore.toFixed(2)),
    priceStrengthScore: Number(priceStrengthScore.toFixed(2)),
    efficiencyScore: Number(efficiencyScore.toFixed(2)),
    expectedDailyRevenue: Number(roi.expectedDailyRevenue.toFixed(2)),
    expectedDailyPowerCost: Number(roi.estimatedDailyPowerCost.toFixed(2)),
    expectedDailyProfit: Number(roi.estimatedDailyMargin.toFixed(2)),
    expectedPaybackMonths: expectedPaybackMonths == null ? null : Number(expectedPaybackMonths.toFixed(2)),
    confidence: {
      level: confidenceLevel,
      bucketCount: points.length,
      score: Number(confidenceScore.toFixed(1)),
      leaseSignalQuality,
    },
    marketSignals: {
      availableShare: Number(avgAvailable.toFixed(4)),
      unavailableShareProxy: Number(avgUnavailable.toFixed(4)),
      activeLeaseShare: Number(avgActiveLease.toFixed(4)),
      newOfferRate: Number((latestPoint.newOfferRate ?? 0).toFixed(4)),
      disappearedRate: Number((latestPoint.disappearedRate ?? 0).toFixed(4)),
      netSupplyChange: latestPoint.netSupplyChange ?? 0,
      marketPressureScore: Number((latestPoint.marketPressureScore ?? 0).toFixed(2)),
      leaseSignalQuality,
    },
    pricing,
    scenarioId: scenario.id,
    scenarioScoreId: persistedScore.id,
    cohort: {
      numGpus: parsed.cohortNumGpus ?? null,
      offerType: parsed.cohortOfferType?.trim().toLowerCase() || null,
    },
    regime,
    recommendationLabel: recommendation.recommendationLabel,
    recommendationReasonPrimary: recommendation.recommendationReasonPrimary,
    recommendationReasonSecondary: recommendation.recommendationReasonSecondary,
    recommendationConfidenceNote: recommendation.recommendationConfidenceNote,
    roi: {
      expectedUtilizationEstimate: Number(roi.expectedUtilizationEstimate.toFixed(4)),
      expectedDailyRevenue: Number(roi.expectedDailyRevenue.toFixed(2)),
      estimatedDailyPowerCost: Number(roi.estimatedDailyPowerCost.toFixed(2)),
      estimatedDailyMargin: Number(roi.estimatedDailyMargin.toFixed(2)),
      paybackPeriodDays: roi.paybackPeriodDays == null ? null : Number(roi.paybackPeriodDays.toFixed(2)),
    },
    trends: {
      window6h: computeWindowTrendSummary(points, latestPoint.bucketStartUtc, "6h"),
      window24h: computeWindowTrendSummary(points, latestPoint.bucketStartUtc, "24h"),
      window7d: computeWindowTrendSummary(points, latestPoint.bucketStartUtc, "7d"),
    },
  };
}

import { subHours } from "date-fns";
import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { calculateScenarioScore } from "@/lib/scoring/score";
import { getRecommendedPriceBands } from "@/lib/scoring/pricing";

const requestSchema = z.object({
  gpuName: z.string().min(1),
  gpuCount: z.number().int().positive().max(128),
  assumedPowerWatts: z.number().int().positive().max(200000),
  assumedHardwareCost: z.number().positive(),
  electricityCostPerKwh: z.number().min(0).max(5),
  targetPaybackMonths: z.number().int().positive().max(120),
  source: z.string().optional().default("vast-live"),
  hoursWindow: z.number().int().min(6).max(168).optional().default(24),
});

function clamp(value: number, min = 0, max = 100): number {
  return Math.max(min, Math.min(max, value));
}

function mean(values: number[]): number {
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function stddev(values: number[]): number {
  if (values.length <= 1) return 0;
  const avg = mean(values);
  const variance = mean(values.map((value) => (value - avg) ** 2));
  return Math.sqrt(variance);
}

function confidenceLabel(bucketCount: number): "low" | "medium" | "high" {
  if (bucketCount >= 24) return "high";
  if (bucketCount >= 12) return "medium";
  return "low";
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
  };
  pricing: {
    aggressive: number;
    target: number;
    premium: number;
  };
  scenarioId: string;
  scenarioScoreId: string;
};

export async function scoreScenarioWithMarket(input: unknown): Promise<MarketScoringResult> {
  const parsed = requestSchema.parse(input);
  const since = subHours(new Date(), parsed.hoursWindow);

  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: {
          gpuName: string;
          source: string;
          bucketStartUtc: { gte: Date };
        };
        orderBy: { bucketStartUtc: "asc" };
      }) => Promise<Array<{
        medianPrice: number | null;
        impliedUtilization: number;
        totalOffers: number;
      }>>;
    };
  }).gpuTrendAggregate;

  if (!trendClient) {
    throw new Error("Trend aggregates are unavailable in the current runtime. Regenerate/restart app.");
  }

  const points = await trendClient.findMany({
    where: {
      gpuName: parsed.gpuName,
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

  const utilizationSeries = points.map((point) => point.impliedUtilization);
  const avgUtilization = mean(utilizationSeries);
  const demandScore = clamp(avgUtilization * 100);

  const totalOfferSeries = points.map((point) => point.totalOffers);
  const avgTotalOffers = mean(totalOfferSeries);
  const competitionScore = clamp(100 - avgTotalOffers * 4);

  const comparablePrices = points
    .map((point) => point.medianPrice)
    .filter((value): value is number => value != null && Number.isFinite(value) && value > 0);

  const avgMedianPrice = comparablePrices.length > 0 ? mean(comparablePrices) : 0;
  const cv = avgMedianPrice > 0 ? stddev(comparablePrices) / avgMedianPrice : 1;
  const priceStrengthScore = clamp(100 - cv * 200);

  const expectedDailyRevenue = avgMedianPrice * parsed.gpuCount * 24 * avgUtilization;
  const expectedDailyPowerCost =
    (parsed.assumedPowerWatts / 1000) * 24 * parsed.electricityCostPerKwh;
  const expectedDailyProfit = expectedDailyRevenue - expectedDailyPowerCost;
  const expectedPaybackMonths =
    expectedDailyProfit > 0
      ? parsed.assumedHardwareCost / expectedDailyProfit / 30.4375
      : null;

  const marginPct = expectedDailyRevenue > 0 ? expectedDailyProfit / expectedDailyRevenue : 0;
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

  const pricing = getRecommendedPriceBands(comparablePrices);

  const scenario = await prisma.hardwareScenario.create({
    data: {
      gpuName: parsed.gpuName,
      gpuCount: parsed.gpuCount,
      assumedPowerWatts: parsed.assumedPowerWatts,
      assumedHardwareCost: parsed.assumedHardwareCost,
      electricityCostPerKwh: parsed.electricityCostPerKwh,
      targetPaybackMonths: parsed.targetPaybackMonths,
      notes: `source=${parsed.source};hoursWindow=${parsed.hoursWindow};buckets=${points.length}`,
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
    expectedDailyRevenue: Number(expectedDailyRevenue.toFixed(2)),
    expectedDailyPowerCost: Number(expectedDailyPowerCost.toFixed(2)),
    expectedDailyProfit: Number(expectedDailyProfit.toFixed(2)),
    expectedPaybackMonths:
      expectedPaybackMonths == null ? null : Number(expectedPaybackMonths.toFixed(2)),
    confidence: {
      level: confidenceLabel(points.length),
      bucketCount: points.length,
    },
    pricing,
    scenarioId: scenario.id,
    scenarioScoreId: persistedScore.id,
  };
}

import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { scoreScenarioWithMarket } from "@/lib/scoring/marketScore";
import { estimatePowerAndCost } from "@/lib/scoring/hardwareDefaults";
import {
  buildTransitionGuidance,
  computeExploratoryOpportunityScore,
  computeReadiness,
  decomposeInferability,
} from "@/lib/scoring/readiness";

const inputSchema = z.object({
  gpuName: z.string().min(1),
  cohortNumGpus: z.number().int().positive().optional(),
  cohortOfferType: z.string().optional(),
  gpuCount: z.number().int().positive().max(128),
  assumedPowerWatts: z.number().int().positive().max(200000),
  assumedHardwareCost: z.number().positive(),
  electricityCostPerKwh: z.number().min(0).max(5),
  targetPaybackMonths: z.number().int().positive().max(120),
  source: z.string().optional(),
  hoursWindow: z.number().int().min(6).max(24 * 21).optional(),
  listingPricePerHour: z.number().positive().optional(),
});

export async function POST(request: Request) {
  const payload = await request.json();
  const parsed = inputSchema.safeParse(payload);

  if (!parsed.success) {
    return Response.json(
      { error: "Invalid scoring payload", issues: parsed.error.issues },
      { status: 400 },
    );
  }

  try {
    const result = await scoreScenarioWithMarket(parsed.data);

    return Response.json({
      ...result,
      overallScore: result.legacy.overallScore,
      demandScore: result.legacy.demandScore,
      competitionScore: result.legacy.competitionScore,
      priceStrengthScore: result.legacy.priceStrengthScore,
      efficiencyScore: result.legacy.efficiencyScore,
      expectedDailyRevenue: result.economics.expectedDailyRevenue,
      expectedDailyPowerCost:
        result.economics.expectedDailyRevenue - result.economics.expectedDailyMargin,
      expectedDailyProfit: result.economics.expectedDailyMargin,
      expectedPaybackMonths: result.economics.expectedPaybackMonths,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown scoring error";
    return Response.json({ error: message }, { status: 422 });
  }
}

export async function GET() {
  const source = "vast-live";

  const cohorts = await prisma.gpuTrendAggregate.findMany({
    where: { source },
    distinct: ["gpuName", "numGpus", "offerType"],
    select: { gpuName: true, numGpus: true, offerType: true },
    orderBy: [{ gpuName: "asc" }, { numGpus: "asc" }, { offerType: "asc" }],
  });

  const latestByCohort = new Map<string, {
    medianPrice: number | null;
    impliedUtilization: number;
    bucketStartUtc: Date;
    cohortPressureScore: number | null;
    state: string | null;
    stateConfidence: number | null;
    uniqueMachines: number | null;
    inferabilityScore: number | null;
    signalStrengthScore: number | null;
    identityQualityScore: number | null;
    churnScore: number | null;
    timeDepthScore: number | null;
    crossSectionDepthScore: number | null;
    dataDepthScore: number | null;
    observationCount: number | null;
    observationsPerOffer: number | null;
    medianPollGapMinutes: number | null;
    maxPollGapMinutes: number | null;
    coverageRatio: number | null;
    offerSeenSpanMinutes: number | null;
    cohortObservationDensityScore: number | null;
    labelabilityScore: number | null;
    futureWindowCoverage12h: number | null;
    futureWindowCoverage24h: number | null;
    futureWindowCoverage72h: number | null;
    samplingQualityScore: number | null;
    lifecycleObservabilityScore: number | null;
    insufficientSampling: boolean | null;
  }>();

  const latestRows = await prisma.gpuTrendAggregate.findMany({
    where: { source },
    orderBy: { bucketStartUtc: "desc" },
    take: 1000,
    select: {
      gpuName: true,
      numGpus: true,
      offerType: true,
      medianPrice: true,
      impliedUtilization: true,
      bucketStartUtc: true,
      cohortPressureScore: true,
      state: true,
      stateConfidence: true,
      uniqueMachines: true,
      inferabilityScore: true,
      signalStrengthScore: true,
      identityQualityScore: true,
      churnScore: true,
      timeDepthScore: true,
      crossSectionDepthScore: true,
      dataDepthScore: true,
      observationCount: true,
      observationsPerOffer: true,
      medianPollGapMinutes: true,
      maxPollGapMinutes: true,
      coverageRatio: true,
      offerSeenSpanMinutes: true,
      cohortObservationDensityScore: true,
      labelabilityScore: true,
      futureWindowCoverage12h: true,
      futureWindowCoverage24h: true,
      futureWindowCoverage72h: true,
      samplingQualityScore: true,
      lifecycleObservabilityScore: true,
      insufficientSampling: true,
    },
  });

  for (const row of latestRows) {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
    if (!latestByCohort.has(key)) {
      latestByCohort.set(key, row);
    }
  }

  const cohortTypeByGpuAndCount = new Map<string, Set<string>>();
  for (const row of cohorts) {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}`;
    const type = (row.offerType ?? "unknown").trim().toLowerCase() || "unknown";
    const current = cohortTypeByGpuAndCount.get(key) ?? new Set<string>();
    current.add(type);
    cohortTypeByGpuAndCount.set(key, current);
  }

  const filtered = cohorts.filter((row) => {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}`;
    const type = (row.offerType ?? "unknown").trim().toLowerCase() || "unknown";
    const types = cohortTypeByGpuAndCount.get(key);
    if (!types) return true;
    return type !== "unknown" || types.size === 1;
  });

  const gpuOptions = filtered.map((row) => {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
    const latest = latestByCohort.get(key);

    return {
      gpuName: row.gpuName,
      cohortNumGpus: row.numGpus,
      cohortOfferType: row.offerType,
      source,
      latestMedianPrice: latest?.medianPrice ?? null,
      latestImpliedUtilization: latest?.impliedUtilization ?? null,
      latestBucketUtc: latest?.bucketStartUtc.toISOString() ?? null,
      latestPressure: latest?.cohortPressureScore ?? null,
      latestState: latest?.state ?? null,
      latestStateConfidence: latest?.stateConfidence ?? null,
      latestUniqueMachines: latest?.uniqueMachines ?? null,
      latestInferabilityScore: latest?.inferabilityScore ?? null,
      latestSignalStrengthScore: latest?.signalStrengthScore ?? null,
      latestIdentityQualityScore: latest?.identityQualityScore ?? null,
      defaults: {
        ...estimatePowerAndCost(row.gpuName),
        gpuCount: row.numGpus ?? 1,
        electricityCostPerKwh: 0.12,
        targetPaybackMonths: 18,
        hoursWindow: 24 * 7,
      },
    };
  });

  const cohortUniverseRaw = filtered
    .map((row) => {
      const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
      const latest = latestByCohort.get(key);
      if (!latest) return null;

      const inferabilityScore = latest.inferabilityScore ?? 0;
      const confidenceScore = latest.stateConfidence ?? 0;
      const identityQualityScore = latest.identityQualityScore ?? 0;
      const samplingQualityScore = latest.samplingQualityScore ?? 0;
      const lifecycleObservabilityScore = latest.lifecycleObservabilityScore ?? 0;
      const churnScore = latest.churnScore ?? 0;
      const timeDepthScore = latest.timeDepthScore ?? Math.min(100, (latest.observationCount ?? 0) / 48);
      const crossSectionDepthScore =
        latest.crossSectionDepthScore ?? Math.min(100, (latest.uniqueMachines ?? 0) * 5);
      const dataDepthScore = latest.dataDepthScore ?? Math.min(100, 45 + (latest.observationCount ?? 0));
      const historyContinuity = timeDepthScore;
      const readiness = computeReadiness({
        inferabilityScore,
        confidenceScore,
        identityQualityScore,
        timeDepthScore,
        crossSectionDepthScore,
        dataDepthScore,
        signalStrengthScore: latest.signalStrengthScore ?? 0,
        churnScore,
        machineBreadth: Math.min(100, (latest.uniqueMachines ?? 0) * 5),
        historyContinuity,
        state: latest.state ?? "balanced",
        observation: {
          observationCount: latest.observationCount ?? 0,
          observationsPerOffer: latest.observationsPerOffer ?? 0,
          medianPollGapMinutes: latest.medianPollGapMinutes ?? 30,
          maxPollGapMinutes: latest.maxPollGapMinutes ?? 30,
          coverageRatio: latest.coverageRatio ?? 0,
          offerSeenSpanMinutes: latest.offerSeenSpanMinutes ?? (latest.observationCount ?? 0) * 30,
          cohortObservationDensityScore: latest.cohortObservationDensityScore ?? 0,
          labelabilityScore: latest.labelabilityScore ?? 0,
          futureWindowCoverage12h: latest.futureWindowCoverage12h ?? 0,
          futureWindowCoverage24h: latest.futureWindowCoverage24h ?? 0,
          futureWindowCoverage72h: latest.futureWindowCoverage72h ?? 0,
          samplingQualityScore,
          lifecycleObservabilityScore,
          insufficientSampling: latest.insufficientSampling ?? false,
        },
      });
      const inferredRecommendation =
        inferabilityScore < 35 || identityQualityScore < 40
          ? "Avoid"
          : readiness.readinessScore >= 78 && inferabilityScore >= 68
            ? "Buy"
            : readiness.readinessScore >= 66
              ? "Buy if discounted"
              : readiness.readinessScore >= 54
                ? "Watch"
                : "Speculative";
      const transitionGuidance = buildTransitionGuidance({
        recommendation: inferredRecommendation,
        inferabilityScore,
        confidenceScore,
        signalStrengthScore: latest.signalStrengthScore ?? 0,
        readinessScore: readiness.readinessScore,
        priceAdvantage: 0,
        churnScore,
      });
      const exploratoryOpportunityScore = computeExploratoryOpportunityScore({
        pressure: latest.cohortPressureScore ?? 0,
        readinessScore: readiness.readinessScore,
        inferabilityScore,
        confidenceScore,
        consumption24h: Math.min(1, Math.max(0, (latest.cohortPressureScore ?? 50) / 100)),
        priceAdvantage: 0,
        churnScore,
        samplingQualityScore,
        identityQualityScore,
      });
      const inferabilityDecomposition = decomposeInferability({
        inferabilityScore,
        samplingQualityScore,
        identityQualityScore,
        dataDepthScore,
        churnScore,
      });

      return {
        key,
        gpuName: row.gpuName,
        cohortNumGpus: row.numGpus,
        cohortOfferType: row.offerType,
        recommendation: inferredRecommendation,
        state: latest.state ?? "balanced",
        confidence: confidenceScore,
        inferability: inferabilityScore,
        readinessScore: readiness.readinessScore,
        readinessBand: readiness.readinessBand,
        readinessTags: readiness.graduationTags,
        pressure: latest.cohortPressureScore ?? 0,
        signalStrengthScore: latest.signalStrengthScore ?? 0,
        samplingQualityScore,
        identityQualityScore,
        lifecycleObservabilityScore,
        observationCount: latest.observationCount ?? 0,
        observationsPerOffer: latest.observationsPerOffer ?? 0,
        medianPollGapMinutes: latest.medianPollGapMinutes ?? 30,
        maxPollGapMinutes: latest.maxPollGapMinutes ?? 30,
        coverageRatio: latest.coverageRatio ?? 0,
        churnScore,
        insufficientSampling: latest.insufficientSampling ?? false,
        exploratoryOpportunityScore,
        nearestUpgrade: transitionGuidance.nearestUpgrade,
        nearestDowngrade: transitionGuidance.nearestDowngrade,
        upgradeGuidance: transitionGuidance.upgradeGuidance,
        downgradeRiskFactors: transitionGuidance.downgradeRiskFactors,
        inferabilityDecomposition,
      };
    })
    .filter((row): row is NonNullable<typeof row> => row != null);

  const sortedExploratory = [...cohortUniverseRaw].sort(
    (a, b) => b.exploratoryOpportunityScore - a.exploratoryOpportunityScore,
  );
  const cohortUniverse = sortedExploratory.map((row, index) => ({
    ...row,
    exploratoryRank: index + 1,
    globalRank: index + 1,
  }));

  const recommendationDistribution = cohortUniverse.reduce<Record<string, number>>((acc, row) => {
    acc[row.recommendation] = (acc[row.recommendation] ?? 0) + 1;
    return acc;
  }, {});
  const regimeDistribution = cohortUniverse.reduce<Record<string, number>>((acc, row) => {
    acc[row.state] = (acc[row.state] ?? 0) + 1;
    return acc;
  }, {});
  const suppressionSummary = {
    suppressedCount: cohortUniverse.filter((row) => row.recommendation === "Avoid").length,
    nearUsableCount: cohortUniverse.filter((row) => row.readinessTags.includes("Near usable")).length,
    underSampledCount: cohortUniverse.filter((row) => row.readinessTags.includes("Under-sampled")).length,
    identityConstrainedCount: cohortUniverse.filter((row) => row.readinessTags.includes("Identity issue")).length,
    churnHeavyCount: cohortUniverse.filter((row) => row.readinessTags.includes("Churn-heavy")).length,
    graduatingSoonCount: cohortUniverse.filter((row) => row.readinessTags.includes("Graduating soon")).length,
  };

  const recentScenarioForecasts = await prisma.scenarioForecast.findMany({
    orderBy: { createdAt: "desc" },
    take: 30,
    include: {
      scenario: {
        select: {
          id: true,
          gpuName: true,
          gpuCount: true,
          targetPaybackMonths: true,
        },
      },
    },
  });

  return Response.json({
    gpuOptions,
    cohortUniverse,
    recommendationDistribution,
    regimeDistribution,
    suppressionSummary,
    pollingSummary: {
      highPriorityTargetMinutes: "2-5",
      generalTargetMinutes: "5-10",
      longTailTargetMinutes: "10-20",
    },
    recentScenarios: recentScenarioForecasts.map((item) => ({
      id: item.scenario.id,
      gpuName: item.scenario.gpuName,
      gpuCount: item.scenario.gpuCount,
      createdAt: item.createdAt,
      targetPaybackMonths: item.scenario.targetPaybackMonths,
      latestScore: {
        overallScore: item.expectedPaybackMonths == null ? 0 : Number((100 / Math.max(item.expectedPaybackMonths, 1)).toFixed(2)),
        recommendation: item.recommendation,
        createdAt: item.createdAt,
      },
      confidenceScore: item.confidenceScore,
      expectedUtilization: item.expectedUtilization,
      expectedPaybackMonths: item.expectedPaybackMonths,
    })),
  });
}

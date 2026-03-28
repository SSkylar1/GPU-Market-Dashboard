import { subHours } from "date-fns";
import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import {
  buildCalibrationBuckets,
  buildRecommendation,
  classifyCohortState,
  clamp,
  computeNoiseScore,
  computeStateConfidence,
  computeWindowTrendSummary,
  estimateConsumptionProbability,
  estimateExpectedUtilization,
  estimateRoiContext,
  forecastProbabilitiesFromState,
  mean,
  safeDiv,
  shrinkTowardsFamily,
  type CohortState,
  type TrendPoint,
} from "@/lib/metrics/intelligence";
import { calculateScenarioScore } from "@/lib/scoring/score";

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
  hoursWindow: z.number().int().min(6).max(24 * 21).optional().default(24 * 7),
  listingPricePerHour: z.number().positive().optional(),
});

const MODEL_VERSION = "predictive-v3";
const CALIBRATION_VERSION = "cal-v1";

type TrendRow = TrendPoint & {
  source: string;
  gpuName: string;
  numGpus: number | null;
  offerType: string | null;
};

export type MarketScoringResult = {
  modelVersion: string;
  calibrationVersion: string;
  scenarioId: string;
  scenarioForecastId: string;
  recommendation: "Buy" | "Buy if discounted" | "Watch" | "Speculative" | "Avoid";
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
  forecastSuppressed: boolean;
  vetoReason: "non_inferable" | "low_inferability" | "identity_quality" | "churn_dominated" | null;
  currentState: {
    state: CohortState;
    pressure: number;
    movementScore: number;
    confidenceScore: number;
    dataDepthScore: number;
    noiseScore: number;
    churnScore: number;
    signalStrengthScore: number;
    inferabilityScore: number;
    identityQualityScore: number;
  };
  exactCohort: {
    gpuName: string;
    numGpus: number | null;
    offerType: string | null;
    latestBucketUtc: string;
    medianPrice: number | null;
    totalOffers: number;
    uniqueMachines: number;
    uniqueHosts: number;
    machineConcentrationShareTop1: number;
    machineConcentrationShareTop3: number;
    hostConcentrationShareTop1: number;
    hostConcentrationShareTop3: number;
    machinePersistenceRate: number;
    hostPersistenceRate: number;
    newMachineEntryRate: number;
    disappearingMachineRate: number;
    persistentDisappearanceRate: number;
    persistentDisappearanceRateN: number;
    temporaryMissingRate: number;
    reappearedShortGapRate: number;
    reappearedLongGapRate: number;
    medianReappearanceDelayBuckets: number | null;
    churnAdjustedDisappearanceRate: number;
    reappearedRate: number;
  };
  familyBaseline: {
    medianPrice: number | null;
    pressure: number;
    hazard: number;
    machineDepth: number;
    confidenceScore: number;
    inferabilityScore: number;
  };
  forecastProbabilities: {
    pTight24h: number;
    pTight72h: number;
    pTight7d: number;
    pPriceUp24h: number;
    pPriceFlat24h: number;
    pPriceDown24h: number;
    pOfferConsumedWithin12h: number;
    pOfferConsumedWithin24h: number;
    pOfferConsumedWithin72h: number;
  };
  utilization: {
    expected: number;
    low: number;
    high: number;
    pAbove25: number;
    pAbove50: number;
    pAbove75: number;
  };
  economics: {
    listingPricePerHour: number;
    relativePriceVsExactMedian: number;
    relativePriceVsFamilyMedian: number;
    expectedDailyRevenue: number;
    expectedDailyRevenueLow: number;
    expectedDailyRevenueHigh: number;
    expectedDailyMargin: number;
    expectedDailyMarginLow: number;
    expectedDailyMarginHigh: number;
    expectedPaybackMonths: number | null;
    expectedPaybackMonthsLow: number | null;
    expectedPaybackMonthsHigh: number | null;
    pPaybackWithinTarget: number;
    pScenarioOutperformingGpuFamilyMedian: number;
  };
  confidence: {
    score: number;
    level: "low" | "medium" | "high";
    bucketCount: number;
    notes: string[];
    forecastSuppressed: boolean;
    vetoReason: "non_inferable" | "low_inferability" | "identity_quality" | "churn_dominated" | null;
    inferabilityScore: number;
    signalStrengthScore: number;
    identityQualityScore: number;
  };
  explanation: {
    observed: string[];
    inferred: string[];
    forecasted: string[];
    risks: string[];
  };
  visuals: {
    pressureTimeline: Array<{
      bucketStartUtc: string;
      pressure: number;
      state: string;
      confidence: number;
      pressureLow: number;
      pressureHigh: number;
    }>;
    supplyTimeline: Array<{
      bucketStartUtc: string;
      totalOffers: number;
      uniqueMachines: number;
      newOffers: number;
      continuingOffers: number;
      disappearedOffers: number;
      reappearedOffers: number;
    }>;
    offerSurvival: Array<{ durationHoursBucket: string; count: number; priceBand: string }>;
    pricePositionCurve: Array<{
      relativePricePosition: number;
      p12h: number;
      p24h: number;
      p72h: number;
      p12hLow: number;
      p12hHigh: number;
      p24hLow: number;
      p24hHigh: number;
      p72hLow: number;
      p72hHigh: number;
    }>;
    configComparison: Array<{
      numGpus: number;
      offerType: string;
      pressure: number;
      hazard: number;
      medianPrice: number | null;
      uniqueMachines: number;
      confidence: number;
    }>;
    marketMap: Array<{
      label: string;
      expectedUtilization: number;
      expectedPayback: number | null;
      bubble: number;
      confidence: number;
      recommendation: string;
    }>;
    calibration: Array<{ bucket: string; count: number; avgPredicted: number; realizedRate: number }>;
  };
  drilldowns: {
    latestOffers: Array<{
      offerId: string;
      machineId: number | null;
      hostId: number | null;
      pricePerHour: number | null;
      reliabilityScore: number | null;
      rentable: boolean;
    }>;
    machineConcentration: Array<{ machineId: number | null; offers: number; share: number }>;
    cohortComparisons: Array<{
      cohort: string;
      pressure: number;
      medianPrice: number | null;
      uniqueMachines: number;
      state: string;
      confidence: number;
    }>;
    backtestCalibrationSummary: Array<{
      horizonHours: number;
      bucket: string;
      count: number;
      realizedRate: number;
      inferabilityBucket?: string | null;
      stateAtPrediction?: string | null;
    }>;
  };
  trends: {
    window6h: ReturnType<typeof computeWindowTrendSummary>;
    window24h: ReturnType<typeof computeWindowTrendSummary>;
    window7d: ReturnType<typeof computeWindowTrendSummary>;
  };
  legacy: {
    overallScore: number;
    recommendation: "Buy" | "Watch" | "Avoid";
    demandScore: number;
    competitionScore: number;
    priceStrengthScore: number;
    efficiencyScore: number;
  };
};

function normalizeOfferType(offerType: string | undefined): string | null {
  if (!offerType) return null;
  const normalized = offerType.trim().toLowerCase();
  return normalized.length === 0 ? null : normalized;
}

function toConfidenceLevel(score: number): "low" | "medium" | "high" {
  if (score >= 68) return "high";
  if (score >= 45) return "medium";
  return "low";
}

function sortByBucket<T extends { bucketStartUtc: Date }>(points: T[]): T[] {
  return [...points].sort((a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime());
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.floor(sorted.length / 2);
  return sorted[index] ?? null;
}

function latestNonNull(values: Array<number | null | undefined>): number | null {
  for (let i = values.length - 1; i >= 0; i -= 1) {
    const value = values[i];
    if (value != null && Number.isFinite(value) && value > 0) return value;
  }
  return null;
}

export async function scoreScenarioWithMarket(input: unknown): Promise<MarketScoringResult> {
  const parsed = requestSchema.parse(input);
  const since = subHours(new Date(), parsed.hoursWindow);
  const normalizedOfferType = normalizeOfferType(parsed.cohortOfferType);

  const exactRows = (await prisma.gpuTrendAggregate.findMany({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
      ...(normalizedOfferType == null ? {} : { offerType: normalizedOfferType }),
      bucketStartUtc: { gte: since },
    },
    orderBy: { bucketStartUtc: "asc" },
  })) as TrendRow[];

  const familyRows = (await prisma.gpuTrendAggregate.findMany({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      numGpus: null,
      offerType: null,
      bucketStartUtc: { gte: since },
    },
    orderBy: { bucketStartUtc: "asc" },
  })) as TrendRow[];

  if (exactRows.length === 0 && familyRows.length === 0) {
    throw new Error(
      `No trend points found for ${parsed.gpuName} in source ${parsed.source} within ${parsed.hoursWindow}h. Run collect/recompute first.`,
    );
  }

  const exactSeries = exactRows.length > 0 ? sortByBucket(exactRows) : sortByBucket(familyRows);
  const familySeries = familyRows.length > 0 ? sortByBucket(familyRows) : sortByBucket(exactSeries);

  const latestExact = exactSeries[exactSeries.length - 1];
  const latestFamily = familySeries[familySeries.length - 1];

  const inferredNoiseScore = latestExact.noiseScore ?? computeNoiseScore(exactSeries);
  const inferredDataDepthScore =
    latestExact.dataDepthScore ??
    clamp((safeDiv(latestExact.totalOffers, 50) + safeDiv(latestExact.uniqueMachines ?? 0, 24)) / 2, 0, 1) * 100;
  const inferredIdentityQualityScore = latestExact.identityQualityScore ?? latestFamily.identityQualityScore ?? 55;
  const inferredMovementScore =
    latestExact.movementScore ??
    clamp(
      (0.45 * (latestExact.disappearedRate ?? 0) +
        0.3 * (latestExact.newOfferRate ?? 0) +
        0.25 * (latestExact.reappearedRate ?? 0)) /
        1.0,
    ) * 100;
  const inferredSignalStrengthScore =
    latestExact.signalStrengthScore ??
    clamp(
      (0.42 * (latestExact.supplyTightnessScore ?? 50) +
        0.18 * (latestExact.machineDepthScore ?? 50) +
        0.12 * inferredIdentityQualityScore +
        0.12 * inferredDataDepthScore -
        0.24 * inferredNoiseScore -
        0.2 * (latestExact.churnScore ?? 50)) / 100,
    ) * 100;
  const inferredInferabilityScore =
    latestExact.inferabilityScore ??
    clamp(
      (0.48 * inferredSignalStrengthScore +
        0.24 * inferredDataDepthScore +
        0.18 * inferredIdentityQualityScore -
        0.3 * inferredNoiseScore -
        0.26 * (latestExact.churnScore ?? 50)) /
        100,
    ) * 100;
  const inferredChurnScore = latestExact.churnScore ?? clamp((latestExact.reappearedRate ?? 0) * 1.2) * 100;

  const inferredState = classifyCohortState({
    dataDepthScore: inferredDataDepthScore,
    noiseScore: inferredNoiseScore,
    cohortPressureScore: latestExact.cohortPressureScore ?? 50,
    pressureAcceleration: latestExact.pressureAcceleration ?? 0,
    persistentDisappearanceRate: latestExact.persistentDisappearanceRate ?? 0,
    reappearedRate: latestExact.reappearedRate ?? 0,
    churnScore: inferredChurnScore,
    signalStrengthScore: inferredSignalStrengthScore,
    inferabilityScore: inferredInferabilityScore,
    identityQualityScore: inferredIdentityQualityScore,
  });

  const state = (latestExact.state as CohortState | null) ?? inferredState;
  const regimeSuppressionFactor =
    state === "non-inferable" ? 0.18 : state === "churn-dominated" ? 0.4 : 1;
  const alignedSignalStrengthScore =
    state === "non-inferable"
      ? Math.min(inferredSignalStrengthScore, 35)
      : state === "churn-dominated"
        ? Math.min(inferredSignalStrengthScore, 52)
        : inferredSignalStrengthScore;
  const alignedInferabilityScore =
    state === "non-inferable"
      ? Math.min(inferredInferabilityScore, 35)
      : state === "churn-dominated"
        ? Math.min(inferredInferabilityScore, 48)
        : inferredInferabilityScore;

  const confidenceScore =
    latestExact.stateConfidence ??
    computeStateConfidence({
      dataDepthScore: inferredDataDepthScore,
      historyDepth: exactSeries.length,
      machineCount: latestExact.uniqueMachines ?? 0,
      noiseScore: inferredNoiseScore,
      reappearedRate: latestExact.reappearedRate ?? 0,
      inferabilityScore: alignedInferabilityScore,
      identityQualityScore: inferredIdentityQualityScore,
    });

  const latestOffersSnapshot = await prisma.marketSnapshot.findFirst({
    where: { source: parsed.source },
    orderBy: { capturedAt: "desc" },
    select: { id: true },
  });

  const latestOffers = latestOffersSnapshot
    ? await prisma.offer.findMany({
        where: {
          snapshotId: latestOffersSnapshot.id,
          gpuName: parsed.gpuName,
          ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
          ...(normalizedOfferType == null ? {} : { offerType: normalizedOfferType }),
        },
        select: {
          offerId: true,
          machineId: true,
          hostId: true,
          pricePerHour: true,
          reliabilityScore: true,
          identityQualityScore: true,
          rentable: true,
        },
      })
    : [];

  const latestOfferMedianPrice = median(
    latestOffers
      .map((offer) => offer.pricePerHour)
      .filter((value): value is number => value != null && Number.isFinite(value) && value > 0),
  );

  const exactHistoryMedian = latestNonNull(exactSeries.map((row) => row.medianPrice));
  const familyHistoryMedian = latestNonNull(familySeries.map((row) => row.medianPrice));
  const exactHistoryBand = latestNonNull([
    ...exactSeries.map((row) => row.p10Price),
    ...exactSeries.map((row) => row.p90Price),
  ]);
  const familyHistoryBand = latestNonNull([
    ...familySeries.map((row) => row.p10Price),
    ...familySeries.map((row) => row.p90Price),
  ]);

  const latestHistoricalOffer = await prisma.offer.findFirst({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      pricePerHour: { not: null },
    },
    orderBy: { capturedAt: "desc" },
    select: { pricePerHour: true },
  });

  const listingPricePerHour =
    parsed.listingPricePerHour ??
    latestExact.medianPrice ??
    latestFamily.medianPrice ??
    exactHistoryMedian ??
    familyHistoryMedian ??
    latestOfferMedianPrice ??
    latestExact.p10Price ??
    latestExact.p90Price ??
    latestFamily.p10Price ??
    latestFamily.p90Price ??
    exactHistoryBand ??
    familyHistoryBand ??
    latestHistoricalOffer?.pricePerHour ??
    1;

  if (listingPricePerHour <= 0) {
    throw new Error(
      "Could not infer a valid listing price from cohort/family/latest-offer history. Provide listingPricePerHour.",
    );
  }

  const relativePriceVsExactMedian =
    latestExact.medianPrice == null || latestExact.medianPrice <= 0
      ? 0
      : (listingPricePerHour - latestExact.medianPrice) / latestExact.medianPrice;
  const relativePriceVsFamilyMedian =
    latestFamily.medianPrice == null || latestFamily.medianPrice <= 0
      ? 0
      : (listingPricePerHour - latestFamily.medianPrice) / latestFamily.medianPrice;

  const shrunkPressure = shrinkTowardsFamily(
    latestExact.cohortPressureScore ?? 50,
    latestFamily.cohortPressureScore ?? 50,
    latestExact.totalOffers,
  );

  const shrunkHazard = shrinkTowardsFamily(
    latestExact.persistentDisappearanceRate ?? latestExact.disappearedRate ?? 0,
    latestFamily.persistentDisappearanceRate ?? latestFamily.disappearedRate ?? 0,
    latestExact.totalOffers,
  );

  const forecast = forecastProbabilitiesFromState({
    state,
    pressure: shrunkPressure,
    pressureAcceleration: latestExact.pressureAcceleration ?? 0,
    confidenceScore,
    configVsFamilyDelta: latestExact.configVsFamilyPressureDelta ?? 0,
    inferabilityScore: alignedInferabilityScore,
    signalStrengthScore: alignedSignalStrengthScore,
  });
  const suppressedTight = (value: number) => clamp(0.24 + (value - 0.24) * regimeSuppressionFactor);
  const suppressedPrice = (value: number) => clamp(0.33 + (value - 0.33) * regimeSuppressionFactor);
  const suppressedForecast = {
    pTight24h: suppressedTight(forecast.pTight24h),
    pTight72h: suppressedTight(forecast.pTight72h),
    pTight7d: suppressedTight(forecast.pTight7d),
    pPriceUp24h: suppressedPrice(forecast.pPriceUp24h),
    pPriceFlat24h: suppressedPrice(forecast.pPriceFlat24h),
    pPriceDown24h: suppressedPrice(forecast.pPriceDown24h),
  };

  const reliabilityMean =
    mean(
      latestOffers
        .map((offer) => offer.reliabilityScore)
        .filter((value): value is number => value != null && Number.isFinite(value)),
    ) || null;
  const latestIdentityQuality =
    mean(
      latestOffers
        .map((offer) => offer.identityQualityScore)
        .filter((value): value is number => value != null && Number.isFinite(value)),
    ) || inferredIdentityQualityScore / 100;
  const identityQualityScore = clamp((latestIdentityQuality ?? 0.55), 0, 1) * 100;

  const pOfferConsumedWithin12h = estimateConsumptionProbability({
    cohortState: state,
    relativePricePosition: relativePriceVsExactMedian,
    reliabilityScore: reliabilityMean,
    pressure: shrunkPressure,
    hours: 12,
    signalStrengthScore: alignedSignalStrengthScore,
    inferabilityScore: alignedInferabilityScore,
  });

  const pOfferConsumedWithin24h = estimateConsumptionProbability({
    cohortState: state,
    relativePricePosition: relativePriceVsExactMedian,
    reliabilityScore: reliabilityMean,
    pressure: shrunkPressure,
    hours: 24,
    signalStrengthScore: alignedSignalStrengthScore,
    inferabilityScore: alignedInferabilityScore,
  });

  const pOfferConsumedWithin72h = estimateConsumptionProbability({
    cohortState: state,
    relativePricePosition: relativePriceVsExactMedian,
    reliabilityScore: reliabilityMean,
    pressure: shrunkPressure,
    hours: 72,
    signalStrengthScore: alignedSignalStrengthScore,
    inferabilityScore: alignedInferabilityScore,
  });
  const suppressedConsumption = (value: number) => clamp(0.48 + (value - 0.48) * regimeSuppressionFactor);
  const pOfferConsumedWithin12hSuppressed = suppressedConsumption(pOfferConsumedWithin12h);
  const pOfferConsumedWithin24hSuppressed = suppressedConsumption(pOfferConsumedWithin24h);
  const pOfferConsumedWithin72hSuppressed = suppressedConsumption(pOfferConsumedWithin72h);

  const utilization = estimateExpectedUtilization({
    cohortState: state,
    pressure: shrunkPressure,
    relativePricePosition: relativePriceVsExactMedian,
    reliabilityScore: reliabilityMean,
    machineDepthScore: latestExact.machineDepthScore ?? 50,
    concentrationScore: latestExact.concentrationScore ?? 50,
    configVsFamilyHazardDelta: latestExact.configVsFamilyHazardDelta ?? 0,
    confidenceScore,
    inferabilityScore: alignedInferabilityScore,
    signalStrengthScore: alignedSignalStrengthScore,
    churnScore: inferredChurnScore,
  });

  const economics = estimateRoiContext({
    utilization,
    listingPricePerHour,
    hardwareCost: parsed.assumedHardwareCost,
    powerWatts: parsed.assumedPowerWatts,
    electricityCostPerKwh: parsed.electricityCostPerKwh,
    targetPaybackMonths: parsed.targetPaybackMonths,
  });

  const familyRelativeRevenue =
    (latestFamily.medianPrice ?? listingPricePerHour) * utilization.expectedUtilization * 24;
  const pScenarioOutperformingGpuFamilyMedian = clamp(
    0.5 + (economics.expectedDailyRevenue - familyRelativeRevenue) / Math.max(familyRelativeRevenue, 1) / 2,
  );

  const recommendation = buildRecommendation({
    pPaybackWithinTarget: economics.pPaybackWithinTarget,
    expectedUtilization: utilization.expectedUtilization,
    confidenceScore,
    cohortState: state,
    concentrationRisk: latestExact.machineConcentrationShareTop3 ?? 0,
    downsideRisk: clamp(1 - economics.pPaybackWithinTarget + (state === "oversupplied" ? 0.15 : 0)),
    inferabilityScore: alignedInferabilityScore,
    identityQualityScore,
    churnScore: inferredChurnScore,
    signalStrengthScore: alignedSignalStrengthScore,
  });

  const scenario = await prisma.hardwareScenario.create({
    data: {
      gpuName: parsed.gpuName,
      gpuCount: parsed.gpuCount,
      assumedPowerWatts: parsed.assumedPowerWatts,
      assumedHardwareCost: parsed.assumedHardwareCost,
      electricityCostPerKwh: parsed.electricityCostPerKwh,
      targetPaybackMonths: parsed.targetPaybackMonths,
      notes: `model=${MODEL_VERSION};source=${parsed.source};window=${parsed.hoursWindow};state=${state}`,
    },
  });

  const scenarioForecast = await prisma.scenarioForecast.create({
    data: {
      hardwareScenarioId: scenario.id,
      modelVersion: MODEL_VERSION,
      source: parsed.source,
      gpuName: parsed.gpuName,
      numGpus: parsed.cohortNumGpus ?? null,
      offerType: normalizedOfferType,
      pUtilizationAbove25: utilization.pUtilizationAbove25,
      pUtilizationAbove50: utilization.pUtilizationAbove50,
      pUtilizationAbove75: utilization.pUtilizationAbove75,
      expectedUtilization: utilization.expectedUtilization,
      expectedUtilizationLow: utilization.expectedUtilizationLow,
      expectedUtilizationHigh: utilization.expectedUtilizationHigh,
      expectedDailyRevenue: economics.expectedDailyRevenue,
      expectedDailyRevenueLow: economics.expectedDailyRevenueLow,
      expectedDailyRevenueHigh: economics.expectedDailyRevenueHigh,
      expectedDailyMargin: economics.expectedDailyMargin,
      expectedDailyMarginLow: economics.expectedDailyMarginLow,
      expectedDailyMarginHigh: economics.expectedDailyMarginHigh,
      expectedPaybackMonths: economics.expectedPaybackMonths,
      expectedPaybackMonthsLow: economics.expectedPaybackMonthsLow,
      expectedPaybackMonthsHigh: economics.expectedPaybackMonthsHigh,
      pPaybackWithinTarget: economics.pPaybackWithinTarget,
      pScenarioOutperformingGpuFamilyMedian,
      confidenceScore,
      recommendation: recommendation.recommendationLabel,
      recommendationReasonPrimary: recommendation.recommendationReasonPrimary,
      recommendationReasonSecondary: recommendation.recommendationReasonSecondary,
      riskFlags: {
        oversupply: state === "oversupplied",
        concentration: (latestExact.machineConcentrationShareTop3 ?? 0) > 0.6,
        thinData: state === "thin-data",
        volatile: state === "volatile",
        churnDominated: state === "churn-dominated",
        nonInferable: state === "non-inferable",
        weakIdentity: identityQualityScore < 50,
        weakPriceSupport: suppressedForecast.pPriceDown24h > suppressedForecast.pPriceUp24h,
        forecastSuppressed: recommendation.forecastSuppressed ?? false,
      },
      explanation: {
        state,
        pressure: shrunkPressure,
        churnScore: inferredChurnScore,
        movementScore: inferredMovementScore,
        signalStrengthScore: alignedSignalStrengthScore,
        inferabilityScore: alignedInferabilityScore,
        forecastSuppressed: recommendation.forecastSuppressed ?? false,
        vetoReason: recommendation.vetoReason ?? null,
        identityQualityScore,
        relativePriceVsExactMedian,
        relativePriceVsFamilyMedian,
      },
    },
  });

  const demandScore = clamp(
    suppressedForecast.pTight24h * 100 + pOfferConsumedWithin24hSuppressed * 15,
    0,
    100,
  );
  const competitionScore = clamp(
    100 - (latestExact.machineConcentrationShareTop3 ?? 0) * 70 - Math.max(0, 45 - (latestExact.machineDepthScore ?? 50)),
    0,
    100,
  );
  const priceStrengthScore = clamp(
    (suppressedForecast.pPriceUp24h - suppressedForecast.pPriceDown24h + 1) * 50,
    0,
    100,
  );
  const efficiencyScore = clamp(
    economics.pPaybackWithinTarget * 70 + utilization.expectedUtilization * 30,
    0,
    100,
  );

  const legacyScore = calculateScenarioScore({
    demandScore,
    competitionScore,
    priceStrengthScore,
    efficiencyScore,
  });

  await prisma.scenarioScore.create({
    data: {
      scenarioId: scenario.id,
      demandScore,
      competitionScore,
      priceStrengthScore,
      efficiencyScore,
      overallScore: legacyScore.overallScore,
      recommendation: legacyScore.recommendation,
      recommendedPriceLow: (latestExact.p10Price ?? listingPricePerHour) * 0.98,
      recommendedPriceTarget: latestExact.medianPrice ?? listingPricePerHour,
      recommendedPriceHigh: (latestExact.p90Price ?? listingPricePerHour) * 1.03,
    },
  });

  const lifecycles = await prisma.offerLifecycle.findMany({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
      ...(normalizedOfferType == null ? {} : { offerType: normalizedOfferType }),
    },
    orderBy: { lastSeenAt: "desc" },
    take: 600,
    select: {
      totalVisibleHours: true,
      latestKnownPricePerHour: true,
      firstKnownPricePerHour: true,
    },
  });

  const survivalBuckets = new Map<string, { count: number; low: number; mid: number; high: number }>();
  for (const lifecycle of lifecycles) {
    const duration = lifecycle.totalVisibleHours;
    const bucket = duration < 4 ? "0-4h" : duration < 12 ? "4-12h" : duration < 24 ? "12-24h" : duration < 72 ? "24-72h" : "72h+";
    const existing = survivalBuckets.get(bucket) ?? { count: 0, low: 0, mid: 0, high: 0 };
    existing.count += 1;

    const start = lifecycle.firstKnownPricePerHour ?? lifecycle.latestKnownPricePerHour;
    const end = lifecycle.latestKnownPricePerHour ?? start;
    const delta = start && end ? safeDiv(end - start, start) : 0;
    if (delta <= -0.03) existing.low += 1;
    else if (delta >= 0.03) existing.high += 1;
    else existing.mid += 1;
    survivalBuckets.set(bucket, existing);
  }

  const offerSurvival = [...survivalBuckets.entries()].flatMap(([durationHoursBucket, counts]) => [
    { durationHoursBucket, count: counts.low, priceBand: "discount" },
    { durationHoursBucket, count: counts.mid, priceBand: "neutral" },
    { durationHoursBucket, count: counts.high, priceBand: "premium" },
  ]);

  const pricePositionCurve = [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3].map((relativePricePosition) => {
    const p12h = estimateConsumptionProbability({
      cohortState: state,
      relativePricePosition,
      reliabilityScore: reliabilityMean,
      pressure: shrunkPressure,
      hours: 12,
      signalStrengthScore: alignedSignalStrengthScore,
      inferabilityScore: alignedInferabilityScore,
    });
    const p24h = estimateConsumptionProbability({
      cohortState: state,
      relativePricePosition,
      reliabilityScore: reliabilityMean,
      pressure: shrunkPressure,
      hours: 24,
      signalStrengthScore: alignedSignalStrengthScore,
      inferabilityScore: alignedInferabilityScore,
    });
    const p72h = estimateConsumptionProbability({
      cohortState: state,
      relativePricePosition,
      reliabilityScore: reliabilityMean,
      pressure: shrunkPressure,
      hours: 72,
      signalStrengthScore: alignedSignalStrengthScore,
      inferabilityScore: alignedInferabilityScore,
    });
    const p12hSupp = clamp(0.48 + (p12h - 0.48) * regimeSuppressionFactor);
    const p24hSupp = clamp(0.48 + (p24h - 0.48) * regimeSuppressionFactor);
    const p72hSupp = clamp(0.48 + (p72h - 0.48) * regimeSuppressionFactor);
    const band = clamp(
      0.06 + (100 - alignedInferabilityScore) / 120 + inferredChurnScore / 240,
      0.06,
      0.38,
    );
    return {
      relativePricePosition,
      p12h: p12hSupp,
      p24h: p24hSupp,
      p72h: p72hSupp,
      p12hLow: clamp(p12hSupp - band),
      p12hHigh: clamp(p12hSupp + band),
      p24hLow: clamp(p24hSupp - band),
      p24hHigh: clamp(p24hSupp + band),
      p72hLow: clamp(p72hSupp - band),
      p72hHigh: clamp(p72hSupp + band),
    };
  });

  const configRows = (await prisma.gpuTrendAggregate.findMany({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      numGpus: { not: null },
      offerType: { not: null },
    },
    distinct: ["numGpus", "offerType"],
    orderBy: [{ numGpus: "asc" }, { offerType: "asc" }, { bucketStartUtc: "desc" }],
    take: 64,
  })) as TrendRow[];

  const configComparison = configRows.map((row) => ({
    numGpus: row.numGpus ?? 1,
    offerType: row.offerType ?? "unknown",
    pressure: row.cohortPressureScore ?? 0,
    hazard: row.persistentDisappearanceRate ?? row.disappearedRate ?? 0,
    medianPrice: row.medianPrice ?? null,
    uniqueMachines: row.uniqueMachines ?? 0,
    confidence: row.stateConfidence ?? 0,
  }));

  const pressureTimeline = exactSeries.map((row) => ({
    bucketStartUtc: row.bucketStartUtc.toISOString(),
    pressure: row.cohortPressureScore ?? 0,
    state: (row.state ?? state) as string,
    confidence: row.stateConfidence ?? confidenceScore,
    pressureLow: Math.max(0, (row.cohortPressureScore ?? 0) - (100 - (row.stateConfidence ?? confidenceScore)) * 0.15),
    pressureHigh: Math.min(100, (row.cohortPressureScore ?? 0) + (100 - (row.stateConfidence ?? confidenceScore)) * 0.15),
  }));

  const supplyTimeline = exactSeries.map((row) => ({
    bucketStartUtc: row.bucketStartUtc.toISOString(),
    totalOffers: row.totalOffers,
    uniqueMachines: row.uniqueMachines ?? 0,
    newOffers: row.newOffers ?? 0,
    continuingOffers: row.continuingOffers ?? 0,
    disappearedOffers: row.disappearedOffers ?? 0,
    reappearedOffers: row.reappearedOffers ?? 0,
  }));

  const forecastBacktests = await prisma.forecastBacktest.findMany({
    where: {
      source: parsed.source,
      gpuName: parsed.gpuName,
      ...(parsed.cohortNumGpus == null ? {} : { numGpus: parsed.cohortNumGpus }),
      ...(normalizedOfferType == null ? {} : { offerType: normalizedOfferType }),
    },
    orderBy: { predictionBucketStartUtc: "desc" },
    take: 400,
  });

  const calibration = buildCalibrationBuckets(
    forecastBacktests.map((row) => ({ predicted: row.predictedPTight, realized: row.realizedTight })),
  );

  const backtestCalibrationSummaryMap = new Map<
    string,
    {
      horizonHours: number;
      bucket: string;
      count: number;
      realizedRate: number;
      inferabilityBucket: string | null;
      stateAtPrediction: string | null;
    }
  >();
  for (const row of forecastBacktests) {
    const key = `${row.horizonHours}:${row.calibrationBucket}:${row.inferabilityBucket ?? "na"}:${row.stateAtPrediction ?? "na"}`;
    const existing = backtestCalibrationSummaryMap.get(key);
    if (!existing) {
      backtestCalibrationSummaryMap.set(key, {
        horizonHours: row.horizonHours,
        bucket: row.calibrationBucket,
        count: 1,
        realizedRate: row.realizedTightSustained ?? row.realizedTight ? 1 : 0,
        inferabilityBucket: row.inferabilityBucket,
        stateAtPrediction: row.stateAtPrediction,
      });
    } else {
      existing.count += 1;
      existing.realizedRate += row.realizedTightSustained ?? row.realizedTight ? 1 : 0;
    }
  }

  const backtestCalibrationSummary = [...backtestCalibrationSummaryMap.values()].map((row) => ({
    ...row,
    realizedRate: safeDiv(row.realizedRate, row.count),
  }));

  const machineCounts = new Map<number | null, number>();
  for (const offer of latestOffers) {
    machineCounts.set(offer.machineId, (machineCounts.get(offer.machineId) ?? 0) + 1);
  }
  const machineConcentration = [...machineCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([machineId, offers]) => ({ machineId, offers, share: safeDiv(offers, latestOffers.length) }));

  const cohortComparisons = configComparison.map((config) => ({
    cohort: `${config.numGpus}x/${config.offerType}`,
    pressure: config.pressure,
    medianPrice: config.medianPrice,
    uniqueMachines: config.uniqueMachines,
    state: config.pressure >= 72 ? "tight" : config.pressure >= 58 ? "tightening" : config.pressure <= 38 ? "oversupplied" : "balanced",
    confidence: config.confidence,
  }));

  const confidenceNotes: string[] = [];
  if (state === "thin-data") confidenceNotes.push("Thin exact-cohort data, heavy shrinkage to family baseline.");
  if (state === "volatile") confidenceNotes.push("High churn variance and reappearance reduce reliability.");
  if (state === "churn-dominated")
    confidenceNotes.push("Churn-dominated regime: movement is high but persistent contraction evidence is weak.");
  if (state === "non-inferable")
    confidenceNotes.push("Non-inferable regime: signal quality is insufficient for decision-grade forecasts.");
  if (alignedInferabilityScore < 45)
    confidenceNotes.push("Inferability is low; probabilities are flattened and uncertainty is widened.");
  if (identityQualityScore < 50)
    confidenceNotes.push("Identity quality is weak; relisting noise may distort lifecycle interpretation.");
  if ((latestExact.machineConcentrationShareTop3 ?? 0) > 0.6)
    confidenceNotes.push("Supply concentration is high, increasing fragility risk.");
  if (confidenceNotes.length === 0) confidenceNotes.push("Data depth and persistence are within normal range.");

  return {
    modelVersion: MODEL_VERSION,
    calibrationVersion: CALIBRATION_VERSION,
    scenarioId: scenario.id,
    scenarioForecastId: scenarioForecast.id,
    recommendation: recommendation.recommendationLabel,
    recommendationReasonPrimary: recommendation.recommendationReasonPrimary,
    recommendationReasonSecondary: recommendation.recommendationReasonSecondary,
    recommendationConfidenceNote: recommendation.recommendationConfidenceNote,
    forecastSuppressed: recommendation.forecastSuppressed ?? false,
    vetoReason: recommendation.vetoReason ?? null,
    currentState: {
      state,
      pressure: Number((shrunkPressure).toFixed(2)),
      movementScore: Number(inferredMovementScore.toFixed(2)),
      confidenceScore: Number(confidenceScore.toFixed(2)),
      dataDepthScore: Number(inferredDataDepthScore.toFixed(2)),
      noiseScore: Number(inferredNoiseScore.toFixed(2)),
      churnScore: Number(inferredChurnScore.toFixed(2)),
      signalStrengthScore: Number(alignedSignalStrengthScore.toFixed(2)),
      inferabilityScore: Number(alignedInferabilityScore.toFixed(2)),
      identityQualityScore: Number(identityQualityScore.toFixed(2)),
    },
    exactCohort: {
      gpuName: parsed.gpuName,
      numGpus: parsed.cohortNumGpus ?? null,
      offerType: normalizedOfferType,
      latestBucketUtc: latestExact.bucketStartUtc.toISOString(),
      medianPrice: latestExact.medianPrice ?? null,
      totalOffers: latestExact.totalOffers,
      uniqueMachines: latestExact.uniqueMachines ?? 0,
      uniqueHosts: latestExact.uniqueHosts ?? 0,
      machineConcentrationShareTop1: latestExact.machineConcentrationShareTop1 ?? 0,
      machineConcentrationShareTop3: latestExact.machineConcentrationShareTop3 ?? 0,
      hostConcentrationShareTop1: latestExact.hostConcentrationShareTop1 ?? 0,
      hostConcentrationShareTop3: latestExact.hostConcentrationShareTop3 ?? 0,
      machinePersistenceRate: latestExact.machinePersistenceRate ?? 0,
      hostPersistenceRate: latestExact.hostPersistenceRate ?? 0,
      newMachineEntryRate: latestExact.newMachineEntryRate ?? 0,
      disappearingMachineRate: latestExact.disappearingMachineRate ?? 0,
      persistentDisappearanceRate: latestExact.persistentDisappearanceRate ?? 0,
      persistentDisappearanceRateN:
        latestExact.persistentDisappearanceRateN ?? latestExact.persistentDisappearanceRate ?? 0,
      temporaryMissingRate: latestExact.temporaryMissingRate ?? 0,
      reappearedShortGapRate: latestExact.reappearedShortGapRate ?? 0,
      reappearedLongGapRate: latestExact.reappearedLongGapRate ?? 0,
      medianReappearanceDelayBuckets: latestExact.medianReappearanceDelayBuckets ?? null,
      churnAdjustedDisappearanceRate:
        latestExact.churnAdjustedDisappearanceRate ?? latestExact.persistentDisappearanceRate ?? 0,
      reappearedRate: latestExact.reappearedRate ?? 0,
    },
    familyBaseline: {
      medianPrice: latestFamily.medianPrice ?? null,
      pressure: latestFamily.cohortPressureScore ?? 0,
      hazard: latestFamily.persistentDisappearanceRate ?? latestFamily.disappearedRate ?? 0,
      machineDepth: latestFamily.machineDepthScore ?? 0,
      confidenceScore: latestFamily.stateConfidence ?? confidenceScore,
      inferabilityScore: latestFamily.inferabilityScore ?? alignedInferabilityScore,
    },
    forecastProbabilities: {
      ...suppressedForecast,
      pOfferConsumedWithin12h: pOfferConsumedWithin12hSuppressed,
      pOfferConsumedWithin24h: pOfferConsumedWithin24hSuppressed,
      pOfferConsumedWithin72h: pOfferConsumedWithin72hSuppressed,
    },
    utilization: {
      expected: utilization.expectedUtilization,
      low: utilization.expectedUtilizationLow,
      high: utilization.expectedUtilizationHigh,
      pAbove25: utilization.pUtilizationAbove25,
      pAbove50: utilization.pUtilizationAbove50,
      pAbove75: utilization.pUtilizationAbove75,
    },
    economics: {
      listingPricePerHour,
      relativePriceVsExactMedian,
      relativePriceVsFamilyMedian,
      expectedDailyRevenue: economics.expectedDailyRevenue,
      expectedDailyRevenueLow: economics.expectedDailyRevenueLow,
      expectedDailyRevenueHigh: economics.expectedDailyRevenueHigh,
      expectedDailyMargin: economics.expectedDailyMargin,
      expectedDailyMarginLow: economics.expectedDailyMarginLow,
      expectedDailyMarginHigh: economics.expectedDailyMarginHigh,
      expectedPaybackMonths: economics.expectedPaybackMonths,
      expectedPaybackMonthsLow: economics.expectedPaybackMonthsLow,
      expectedPaybackMonthsHigh: economics.expectedPaybackMonthsHigh,
      pPaybackWithinTarget: economics.pPaybackWithinTarget,
      pScenarioOutperformingGpuFamilyMedian,
    },
    confidence: {
      score: Number(confidenceScore.toFixed(2)),
      level: toConfidenceLevel(confidenceScore),
      bucketCount: exactSeries.length,
      notes: confidenceNotes,
      forecastSuppressed: recommendation.forecastSuppressed ?? false,
      vetoReason: recommendation.vetoReason ?? null,
      inferabilityScore: Number(alignedInferabilityScore.toFixed(2)),
      signalStrengthScore: Number(alignedSignalStrengthScore.toFixed(2)),
      identityQualityScore: Number(identityQualityScore.toFixed(2)),
    },
    explanation: {
      observed: [
        `Visible offers: ${latestExact.totalOffers}`,
        `Unique machines: ${latestExact.uniqueMachines ?? 0}`,
        `Median visible price: ${latestExact.medianPrice == null ? "n/a" : `$${latestExact.medianPrice.toFixed(3)}/h`}`,
      ],
      inferred: [
        `Cohort state inferred as ${state}`,
        `Persistent disappearance rate ${(100 * (latestExact.persistentDisappearanceRateN ?? latestExact.persistentDisappearanceRate ?? 0)).toFixed(1)}%`,
        `Churn score ${inferredChurnScore.toFixed(1)} / 100`,
        `Movement ${inferredMovementScore.toFixed(1)} / 100`,
        `Signal strength ${alignedSignalStrengthScore.toFixed(1)} / 100 | Inferability ${alignedInferabilityScore.toFixed(1)} / 100`,
        `Config-vs-family pressure delta ${(latestExact.configVsFamilyPressureDelta ?? 0).toFixed(2)}`,
      ],
      forecasted: [
        `P(tight 24h) ${(suppressedForecast.pTight24h * 100).toFixed(1)}%`,
        `P(consumed in 24h) ${(pOfferConsumedWithin24hSuppressed * 100).toFixed(1)}% at current price position`,
        `Expected utilization ${(utilization.expectedUtilization * 100).toFixed(1)}%`,
      ],
      risks: [
        state === "non-inferable" ? "Non-inferable regime suppresses strong recommendations" : "Inferability available",
        state === "churn-dominated" ? "Churn-dominated market can mimic demand without true contraction" : "No dominant churn-only regime",
        state === "oversupplied" ? "Oversupply risk" : "No dominant oversupply signal",
        (latestExact.machineConcentrationShareTop3 ?? 0) > 0.6
          ? "High concentration in few machines"
          : "Machine concentration moderate",
        alignedInferabilityScore < 45 ? "Low inferability: forecasts flattened and uncertainty widened" : "Inferability acceptable",
      ],
    },
    visuals: {
      pressureTimeline,
      supplyTimeline,
      offerSurvival,
      pricePositionCurve,
      configComparison,
      marketMap: [
        {
          label: "Scenario",
          expectedUtilization: utilization.expectedUtilization,
          expectedPayback: economics.expectedPaybackMonths,
          bubble: Math.max(8, Math.round((latestExact.uniqueMachines ?? 0) * 1.1)),
          confidence: confidenceScore,
          recommendation: recommendation.recommendationLabel,
        },
      ],
      calibration,
    },
    drilldowns: {
      latestOffers: latestOffers.slice(0, 100),
      machineConcentration,
      cohortComparisons,
      backtestCalibrationSummary,
    },
    trends: {
      window6h: computeWindowTrendSummary(exactSeries, latestExact.bucketStartUtc, "6h"),
      window24h: computeWindowTrendSummary(exactSeries, latestExact.bucketStartUtc, "24h"),
      window7d: computeWindowTrendSummary(exactSeries, latestExact.bucketStartUtc, "7d"),
    },
    legacy: {
      overallScore: legacyScore.overallScore,
      recommendation: legacyScore.recommendation,
      demandScore: Number(demandScore.toFixed(2)),
      competitionScore: Number(competitionScore.toFixed(2)),
      priceStrengthScore: Number(priceStrengthScore.toFixed(2)),
      efficiencyScore: Number(efficiencyScore.toFixed(2)),
    },
  };
}

import { subDays, subHours } from "date-fns";

export type CohortState =
  | "oversupplied"
  | "balanced"
  | "tightening"
  | "tight"
  | "volatile"
  | "thin-data"
  | "churn-dominated"
  | "non-inferable";

export type Regime = "tight" | "balanced" | "oversupplied" | "churn-dominated" | "non-inferable";

export type TrendPoint = {
  bucketStartUtc: Date;
  totalOffers: number;
  uniqueMachines?: number | null;
  uniqueHosts?: number | null;
  rentableOffers: number;
  rentedOffers?: number;
  continuingOffers?: number | null;
  newOffers?: number | null;
  disappearedOffers?: number | null;
  reappearedOffers?: number | null;
  persistentDisappearedOffers?: number | null;
  newOfferRate?: number | null;
  disappearedRate?: number | null;
  reappearedRate?: number | null;
  temporaryMissingRate?: number | null;
  reappearedShortGapRate?: number | null;
  reappearedLongGapRate?: number | null;
  medianReappearanceDelayBuckets?: number | null;
  persistentDisappearanceRate?: number | null;
  persistentDisappearanceRateN?: number | null;
  churnAdjustedDisappearanceRate?: number | null;
  medianPrice?: number | null;
  minPrice?: number | null;
  p10Price?: number | null;
  p90Price?: number | null;
  maxPrice?: number | null;
  priceCv?: number | null;
  medianPriceChange?: number | null;
  lowBandDisappearedRate?: number | null;
  midBandDisappearedRate?: number | null;
  highBandDisappearedRate?: number | null;
  lowBandPersistentDisappearedRate?: number | null;
  midBandPersistentDisappearedRate?: number | null;
  highBandPersistentDisappearedRate?: number | null;
  machineConcentrationShareTop1?: number | null;
  machineConcentrationShareTop3?: number | null;
  hostConcentrationShareTop1?: number | null;
  hostConcentrationShareTop3?: number | null;
  machinePersistenceRate?: number | null;
  hostPersistenceRate?: number | null;
  newMachineEntryRate?: number | null;
  disappearingMachineRate?: number | null;
  supplyTightnessScore?: number | null;
  movementScore?: number | null;
  machineDepthScore?: number | null;
  concentrationScore?: number | null;
  cohortPressureScore?: number | null;
  pressureAcceleration?: number | null;
  pressurePersistence?: number | null;
  state?: string | null;
  stateConfidence?: number | null;
  dataDepthScore?: number | null;
  noiseScore?: number | null;
  churnScore?: number | null;
  signalStrengthScore?: number | null;
  inferabilityScore?: number | null;
  identityQualityScore?: number | null;
  configVsFamilyPressureDelta?: number | null;
  configVsFamilyPriceDelta?: number | null;
  configVsFamilyHazardDelta?: number | null;
};

export type CompetitionMetrics = {
  distinctHosts: number;
  distinctMachines: number;
  offersPerHost: number;
  offersPerMachine: number;
  topHostShare: number;
  top5HostShare: number;
  hostConcentrationIndex: number;
  hostConcentrationShareTop1: number;
  hostConcentrationShareTop3: number;
  machineConcentrationShareTop1: number;
  machineConcentrationShareTop3: number;
  avgReliabilityScore: number | null;
};

export type CohortPressureBreakdown = {
  cohortPressureScore: number;
  movementScore: number;
  supplyTightnessScore: number;
  machineDepthScore: number;
  concentrationScore: number;
  pressureAcceleration: number;
  pressurePersistence: number;
  churnScore: number;
  signalStrengthScore: number;
  inferabilityScore: number;
  churnAdjustedDisappearanceRate: number;
};

export type ForecastProbabilities = {
  pTight24h: number;
  pTight72h: number;
  pTight7d: number;
  pPriceUp24h: number;
  pPriceFlat24h: number;
  pPriceDown24h: number;
};

export type UtilizationDistribution = {
  expectedUtilization: number;
  expectedUtilizationLow: number;
  expectedUtilizationHigh: number;
  pUtilizationAbove25: number;
  pUtilizationAbove50: number;
  pUtilizationAbove75: number;
};

export type EconomicsDistribution = {
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
};

export type Recommendation = {
  recommendationLabel: "Buy" | "Buy if discounted" | "Watch" | "Speculative" | "Avoid";
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
  forecastSuppressed?: boolean;
  vetoReason?: "non_inferable" | "low_inferability" | "identity_quality" | "churn_dominated" | null;
};

export type MetricTrend = {
  latest: number;
  trailingAverage: number;
  absoluteChange: number;
  percentChange: number | null;
  slopePerBucket: number;
  direction: "up" | "down" | "flat";
};

export type WindowTrendSummary = {
  pointCount: number;
  supply: MetricTrend | null;
  pressure: MetricTrend | null;
  marketPressureScore: MetricTrend | null;
  disappearedRate: MetricTrend | null;
  persistentDisappearanceRate: MetricTrend | null;
  medianPrice: MetricTrend | null;
  uniqueMachines: MetricTrend | null;
};

export function clamp(value: number, min = 0, max = 1): number {
  return Math.max(min, Math.min(max, value));
}

export function safeDiv(numerator: number, denominator: number, fallback = 0): number {
  if (!Number.isFinite(denominator) || denominator === 0) return fallback;
  return numerator / denominator;
}

export function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil(p * sorted.length) - 1;
  const safeIndex = Math.min(sorted.length - 1, Math.max(0, idx));
  return sorted[safeIndex];
}

export function logistic(value: number): number {
  return 1 / (1 + Math.exp(-value));
}

export function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function stddev(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  return Math.sqrt(mean(values.map((v) => (v - m) ** 2)));
}

function slope(values: number[]): number {
  if (values.length < 2) return 0;
  const n = values.length;
  const xMean = (n - 1) / 2;
  const yMean = mean(values);
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = i - xMean;
    num += dx * (values[i] - yMean);
    den += dx * dx;
  }
  return den === 0 ? 0 : num / den;
}

export function buildMetricTrend(values: number[]): MetricTrend | null {
  if (values.length === 0) return null;
  const latest = values[values.length - 1];
  const first = values[0];
  const abs = latest - first;
  const pct = Math.abs(first) <= 1e-9 ? null : (abs / first) * 100;
  const sl = slope(values);

  return {
    latest,
    trailingAverage: mean(values),
    absoluteChange: abs,
    percentChange: pct,
    slopePerBucket: sl,
    direction: Math.abs(sl) < 1e-6 ? "flat" : sl > 0 ? "up" : "down",
  };
}

export function computeWindowTrendSummary(
  points: TrendPoint[],
  anchorBucketUtc: Date,
  window: "6h" | "24h" | "7d",
): WindowTrendSummary {
  const windowStart =
    window === "6h"
      ? subHours(anchorBucketUtc, 6)
      : window === "24h"
        ? subHours(anchorBucketUtc, 24)
        : subDays(anchorBucketUtc, 7);

  const filtered = points
    .filter((point) => point.bucketStartUtc >= windowStart && point.bucketStartUtc <= anchorBucketUtc)
    .sort((a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime());

  return {
    pointCount: filtered.length,
    supply: buildMetricTrend(filtered.map((point) => point.totalOffers)),
    pressure: buildMetricTrend(
      filtered.map((point) => point.cohortPressureScore ?? (point as { marketPressureScore?: number | null }).marketPressureScore ?? 0),
    ),
    marketPressureScore: buildMetricTrend(
      filtered.map((point) => point.cohortPressureScore ?? (point as { marketPressureScore?: number | null }).marketPressureScore ?? 0),
    ),
    disappearedRate: buildMetricTrend(filtered.map((point) => point.disappearedRate ?? 0)),
    persistentDisappearanceRate: buildMetricTrend(
      filtered.map((point) => point.persistentDisappearanceRate ?? 0),
    ),
    medianPrice: buildMetricTrend(filtered.map((point) => point.medianPrice ?? 0)),
    uniqueMachines: buildMetricTrend(filtered.map((point) => point.uniqueMachines ?? 0)),
  };
}

export function computeCompetitionMetrics(
  offers: Array<{ hostId: number | null; machineId: number | null; reliabilityScore: number | null }>,
): CompetitionMetrics {
  const hostCounts = new Map<number, number>();
  const machineCounts = new Map<number, number>();
  const reliabilities: number[] = [];

  for (const offer of offers) {
    if (offer.hostId != null) hostCounts.set(offer.hostId, (hostCounts.get(offer.hostId) ?? 0) + 1);
    if (offer.machineId != null)
      machineCounts.set(offer.machineId, (machineCounts.get(offer.machineId) ?? 0) + 1);
    if (offer.reliabilityScore != null) reliabilities.push(offer.reliabilityScore);
  }

  const total = offers.length;
  const hostShares = [...hostCounts.values()].sort((a, b) => b - a).map((count) => safeDiv(count, total));
  const machineShares = [...machineCounts.values()]
    .sort((a, b) => b - a)
    .map((count) => safeDiv(count, total));

  return {
    distinctHosts: hostCounts.size,
    distinctMachines: machineCounts.size,
    offersPerHost: safeDiv(total, Math.max(hostCounts.size, 1)),
    offersPerMachine: safeDiv(total, Math.max(machineCounts.size, 1)),
    topHostShare: hostShares[0] ?? 0,
    top5HostShare:
      (hostShares[0] ?? 0) +
      (hostShares[1] ?? 0) +
      (hostShares[2] ?? 0) +
      (hostShares[3] ?? 0) +
      (hostShares[4] ?? 0),
    hostConcentrationIndex: hostShares.reduce((acc, share) => acc + share * share, 0),
    hostConcentrationShareTop1: hostShares[0] ?? 0,
    hostConcentrationShareTop3: (hostShares[0] ?? 0) + (hostShares[1] ?? 0) + (hostShares[2] ?? 0),
    machineConcentrationShareTop1: machineShares[0] ?? 0,
    machineConcentrationShareTop3:
      (machineShares[0] ?? 0) + (machineShares[1] ?? 0) + (machineShares[2] ?? 0),
    avgReliabilityScore: reliabilities.length === 0 ? null : mean(reliabilities),
  };
}

export function deriveMarketPressureFromPair(input: {
  newOfferRate: number;
  disappearedRate: number;
  medianPriceChange: number;
  priorMedianPrice: number | null;
  rentableShareChange: number;
}) {
  const priceChangePct =
    input.priorMedianPrice == null || input.priorMedianPrice <= 0
      ? 0
      : input.medianPriceChange / input.priorMedianPrice;

  const churnComponent = clamp(input.disappearedRate);
  const supplyComponent = clamp(1 - input.newOfferRate);
  const priceComponent = clamp((priceChangePct + 0.15) / 0.35);
  const availabilityComponent = clamp((-input.rentableShareChange + 0.2) / 0.45);

  const marketPressureScore =
    100 * (0.35 * churnComponent + 0.25 * supplyComponent + 0.2 * priceComponent + 0.2 * availabilityComponent);

  return {
    marketPressureScore,
    marketPressureChurnComponent: churnComponent * 100,
    marketPressureSupplyComponent: supplyComponent * 100,
    marketPressurePriceComponent: priceComponent * 100,
    marketPressureAvailabilityComponent: availabilityComponent * 100,
  };
}

export function computeCohortPressureScore(input: {
  persistentDisappearanceRate: number;
  disappearedRate: number;
  newOfferRate: number;
  lowBandPersistentDisappearedRate: number;
  medianPriceChangePct: number;
  rentableShare: number;
  uniqueMachineCount: number;
  machineConcentrationShareTop3: number;
  reappearedRate: number;
  temporaryMissingRate: number;
  identityQualityScore: number;
  priorPressure: number;
  priorPressure2: number;
}): CohortPressureBreakdown {
  // Movement can be real demand or churn noise; we model them separately.
  const movementScore = 100 * clamp(
    0.44 * input.disappearedRate +
      0.28 * input.newOfferRate +
      0.18 * input.reappearedRate +
      0.1 * input.temporaryMissingRate,
  );

  const churnScore = 100 * clamp(
    0.25 * input.disappearedRate +
      0.45 * input.reappearedRate +
      0.3 * input.temporaryMissingRate,
  );

  const persistenceSignal = clamp(
    0.55 * input.persistentDisappearanceRate + 0.45 * input.lowBandPersistentDisappearedRate,
  );
  const churnPenalty = clamp(churnScore / 100 - 0.6 * persistenceSignal);
  const churnAdjustedDisappearanceRate = clamp(
    input.persistentDisappearanceRate * (1 - 0.75 * churnPenalty),
  );

  const supplyTightnessScore = clamp(
    0.48 * churnAdjustedDisappearanceRate +
      0.1 * input.disappearedRate +
      0.12 * (1 - input.newOfferRate) +
      0.3 * input.lowBandPersistentDisappearedRate,
    0,
    1,
  );

  const machineDepthScore = clamp(safeDiv(input.uniqueMachineCount, 24));
  const concentrationPenalty = clamp(input.machineConcentrationShareTop3);
  const concentrationScore = 1 - concentrationPenalty;
  const priceSignal = clamp((input.medianPriceChangePct + 0.08) / 0.2);
  const availabilitySignal = clamp(1 - input.rentableShare);
  const identitySignal = clamp(input.identityQualityScore / 100);
  const signalStrengthScore =
    100 *
    clamp(
      0.4 * supplyTightnessScore +
        0.15 * priceSignal +
        0.15 * availabilitySignal +
        0.15 * machineDepthScore +
        0.05 * concentrationScore +
        0.1 * identitySignal -
        0.45 * churnPenalty,
    );
  const inferabilityScore =
    100 *
    clamp(
      0.5 * (signalStrengthScore / 100) +
        0.2 * identitySignal +
        0.3 * (1 - churnPenalty),
    );

  const basePressure =
    100 *
    clamp(
      0.52 * supplyTightnessScore +
        0.16 * priceSignal +
        0.12 * availabilitySignal +
        0.1 * machineDepthScore +
        0.1 * concentrationScore,
      0,
      1,
    );
  // In churn-heavy regimes, collapse toward neutral to avoid false tightness.
  const suppression = clamp(1 - 0.75 * churnPenalty - 0.3 * input.temporaryMissingRate, 0.15, 1);
  const rawPressure = 50 + (basePressure - 50) * suppression;

  const pressureAcceleration = (rawPressure - input.priorPressure) * suppression;
  const pressurePersistence = mean([rawPressure, input.priorPressure, input.priorPressure2]);

  return {
    cohortPressureScore: rawPressure,
    movementScore,
    supplyTightnessScore: supplyTightnessScore * 100,
    machineDepthScore: machineDepthScore * 100,
    concentrationScore: concentrationScore * 100,
    pressureAcceleration,
    pressurePersistence,
    churnScore,
    signalStrengthScore,
    inferabilityScore,
    churnAdjustedDisappearanceRate,
  };
}

export function classifyCohortState(input: {
  dataDepthScore: number;
  noiseScore: number;
  cohortPressureScore: number;
  pressureAcceleration: number;
  persistentDisappearanceRate: number;
  reappearedRate: number;
  churnScore?: number;
  signalStrengthScore?: number;
  inferabilityScore?: number;
  identityQualityScore?: number;
}): CohortState {
  const churnScore = input.churnScore ?? 0;
  const signalStrengthScore = input.signalStrengthScore ?? 50;
  const inferabilityScore = input.inferabilityScore ?? 50;
  const identityQualityScore = input.identityQualityScore ?? 60;
  if (
    inferabilityScore < 42 ||
    identityQualityScore < 42 ||
    input.dataDepthScore < 30 ||
    input.noiseScore > 92
  ) {
    return "non-inferable";
  }
  if (churnScore >= 62 && signalStrengthScore < 50 && input.persistentDisappearanceRate < 0.18)
    return "churn-dominated";
  if (input.dataDepthScore < 35) return "thin-data";
  if (input.noiseScore > 60 || input.reappearedRate > 0.35) return "volatile";
  if (input.cohortPressureScore >= 72 && input.persistentDisappearanceRate >= 0.2) return "tight";
  if (input.cohortPressureScore >= 58 || input.pressureAcceleration >= 4) return "tightening";
  if (input.cohortPressureScore <= 38 && input.persistentDisappearanceRate < 0.08) return "oversupplied";
  return "balanced";
}

export function computeStateConfidence(input: {
  dataDepthScore: number;
  historyDepth: number;
  machineCount: number;
  noiseScore: number;
  reappearedRate: number;
  inferabilityScore?: number;
  identityQualityScore?: number;
}): number {
  const historyScore = clamp(safeDiv(input.historyDepth, 48));
  const machineScore = clamp(safeDiv(input.machineCount, 16));
  const qualityPenalty = clamp((input.noiseScore + input.reappearedRate * 100) / 140);
  const inferability = clamp((input.inferabilityScore ?? 55) / 100);
  const identity = clamp((input.identityQualityScore ?? 60) / 100);

  return (
    100 *
    clamp(
      0.32 * (input.dataDepthScore / 100) +
        0.23 * historyScore +
        0.18 * machineScore +
        0.15 * inferability +
        0.1 * identity -
        0.42 * qualityPenalty,
      0,
      1,
    )
  );
}

export function classifyMarketRegime(input: {
  disappearedRate: number;
  newOfferRate: number;
  netSupplyChange: number;
  medianPriceChange: number;
  rentableShareChange: number;
  marketPressureScore: number;
  reappearedRate?: number;
  persistentDisappearanceRate?: number;
  inferabilityScore?: number;
  dataDepthScore?: number;
}): Regime {
  if ((input.inferabilityScore ?? 55) < 32 || (input.dataDepthScore ?? 60) < 25) return "non-inferable";
  if (
    (input.reappearedRate ?? 0) >= 0.28 &&
    input.disappearedRate >= 0.2 &&
    (input.persistentDisappearanceRate ?? 0) < 0.12 &&
    Math.abs(input.netSupplyChange) <= 1
  ) {
    return "churn-dominated";
  }

  const isTight =
    input.marketPressureScore >= 70 &&
    input.disappearedRate >= 0.2 &&
    input.netSupplyChange <= 0 &&
    input.medianPriceChange >= 0 &&
    input.rentableShareChange <= 0;

  if (isTight) return "tight";

  const isOversupplied =
    input.marketPressureScore <= 40 &&
    input.newOfferRate >= 0.22 &&
    input.netSupplyChange >= 0 &&
    input.medianPriceChange <= 0;

  if (isOversupplied) return "oversupplied";
  return "balanced";
}

export function shrinkTowardsFamily(exactValue: number, familyValue: number, effectiveSampleSize: number): number {
  const weight = clamp(safeDiv(effectiveSampleSize, effectiveSampleSize + 24));
  return exactValue * weight + familyValue * (1 - weight);
}

export function forecastProbabilitiesFromState(input: {
  state: CohortState;
  pressure: number;
  pressureAcceleration: number;
  confidenceScore: number;
  configVsFamilyDelta: number;
  inferabilityScore?: number;
  signalStrengthScore?: number;
}): ForecastProbabilities {
  const tightBiasByState: Record<CohortState, number> = {
    tight: 1.1,
    tightening: 0.55,
    balanced: 0,
    oversupplied: -0.8,
    volatile: -0.15,
    "thin-data": -0.35,
    "churn-dominated": -0.25,
    "non-inferable": -0.55,
  };

  const base =
    tightBiasByState[input.state] +
    (input.pressure - 50) / 22 +
    input.pressureAcceleration / 18 +
    input.configVsFamilyDelta / 20;

  const confidenceDamp = 0.35 + 0.65 * (input.confidenceScore / 100);
  const inferabilityDamp = 0.25 + 0.75 * ((input.inferabilityScore ?? input.confidenceScore) / 100);
  const signalDamp = 0.35 + 0.65 * ((input.signalStrengthScore ?? input.confidenceScore) / 100);
  let totalDamp = confidenceDamp * inferabilityDamp * signalDamp;
  if (input.state === "churn-dominated") totalDamp *= 0.55;
  if (input.state === "non-inferable") totalDamp *= 0.25;

  const pTight24h = clamp(logistic(base) * totalDamp + 0.22 * (1 - totalDamp));
  const pTight72h = clamp(logistic(base * 0.82) * totalDamp + 0.24 * (1 - totalDamp));
  const pTight7d = clamp(logistic(base * 0.62) * totalDamp + 0.26 * (1 - totalDamp));

  const priceLift = clamp(logistic((input.pressure - 55) / 18 + input.pressureAcceleration / 20) * totalDamp);
  const pPriceUp24h = clamp(0.15 + priceLift * 0.7);
  const pPriceDown24h = clamp(0.15 + (1 - priceLift) * 0.45);
  const pPriceFlat24h = clamp(1 - pPriceUp24h - pPriceDown24h);

  return {
    pTight24h,
    pTight72h,
    pTight7d,
    pPriceUp24h,
    pPriceFlat24h,
    pPriceDown24h,
  };
}

export function estimateConsumptionProbability(input: {
  cohortState: CohortState;
  relativePricePosition: number;
  reliabilityScore: number | null;
  pressure: number;
  hours: number;
  signalStrengthScore?: number;
  inferabilityScore?: number;
}): number {
  const stateBias: Record<CohortState, number> = {
    tight: 0.9,
    tightening: 0.5,
    balanced: 0.1,
    oversupplied: -0.6,
    volatile: -0.1,
    "thin-data": -0.25,
    "churn-dominated": -0.2,
    "non-inferable": -0.4,
  };

  const signalStrength = clamp((input.signalStrengthScore ?? 55) / 100);
  const inferability = clamp((input.inferabilityScore ?? 55) / 100);
  const stateSlopeMultiplier =
    input.cohortState === "non-inferable" ? 0.22 : input.cohortState === "churn-dominated" ? 0.45 : 1;
  const slopeStrength = (0.15 + 0.85 * signalStrength * inferability) * stateSlopeMultiplier;
  const relPricePenalty = input.relativePricePosition * 1.25 * slopeStrength;
  const reliability = input.reliabilityScore == null ? 0 : (input.reliabilityScore - 0.95) / 0.08;
  const horizonScale = Math.log(Math.max(2, input.hours)) / Math.log(12);

  let neutralPull = (1 - signalStrength * inferability) * 0.7;
  if (input.cohortState === "churn-dominated") neutralPull = Math.max(neutralPull, 0.62);
  if (input.cohortState === "non-inferable") neutralPull = Math.max(neutralPull, 0.82);
  const z =
    stateBias[input.cohortState] +
    (input.pressure - 50) / 20 -
    relPricePenalty +
    reliability +
    0.28 * horizonScale;
  const p = logistic(z);
  const neutralTarget = input.cohortState === "non-inferable" ? 0.46 : 0.5;
  return clamp(p * (1 - neutralPull) + neutralTarget * neutralPull);
}

function estimateExpectedUtilizationLegacy(input: {
  disappearedRate: number;
  netSupplyChange: number;
  visibleSupplyCount: number;
  rentableShare: number;
  listingPricePerHour: number;
  medianPrice: number | null;
  reliabilityScore: number | null;
}): number {
  const churnSignal = clamp(input.disappearedRate * 2);
  const netRate = safeDiv(-input.netSupplyChange, Math.max(input.visibleSupplyCount, 1));
  const supplySignal = clamp((netRate + 1) / 2);
  const availabilitySignal = clamp(1 - input.rentableShare);
  const priceSignal =
    input.medianPrice == null || input.medianPrice <= 0
      ? 0.5
      : clamp(1 - (input.listingPricePerHour - input.medianPrice) / input.medianPrice);
  const reliabilitySignal =
    input.reliabilityScore == null ? 0.5 : clamp((input.reliabilityScore - 0.9) / 0.1);

  const estimate =
    0.1 +
    churnSignal * 0.28 +
    supplySignal * 0.2 +
    availabilitySignal * 0.22 +
    priceSignal * 0.2 +
    reliabilitySignal * 0.1;

  return clamp(estimate, 0, 0.98);
}

function estimateExpectedUtilizationDistribution(input: {
  cohortState: CohortState;
  pressure: number;
  relativePricePosition: number;
  reliabilityScore: number | null;
  machineDepthScore: number;
  concentrationScore: number;
  configVsFamilyHazardDelta: number;
  confidenceScore: number;
  inferabilityScore?: number;
  signalStrengthScore?: number;
  churnScore?: number;
}): UtilizationDistribution {
  const stateBase: Record<CohortState, number> = {
    tight: 0.72,
    tightening: 0.58,
    balanced: 0.46,
    oversupplied: 0.29,
    volatile: 0.38,
    "thin-data": 0.34,
    "churn-dominated": 0.31,
    "non-inferable": 0.26,
  };

  const inferability = clamp((input.inferabilityScore ?? input.confidenceScore) / 100);
  const signalStrength = clamp((input.signalStrengthScore ?? input.confidenceScore) / 100);
  const churnPenalty = clamp((input.churnScore ?? 40) / 100);
  const shrinkToBaseline = 1 - inferability;
  const conservativeBaseline =
    input.cohortState === "non-inferable" ? 0.24 : input.cohortState === "churn-dominated" ? 0.28 : 0.32;
  const reliability = input.reliabilityScore == null ? 0 : (input.reliabilityScore - 0.95) * 0.7;
  let meanUtil = clamp(
    stateBase[input.cohortState] +
      (input.pressure - 50) / 180 -
      input.relativePricePosition * 0.28 +
      (input.machineDepthScore - 50) / 300 +
      (input.concentrationScore - 50) / 350 +
      input.configVsFamilyHazardDelta / 120 +
      reliability,
    0.03,
    0.97,
  );
  meanUtil = clamp(meanUtil * (1 - 0.55 * shrinkToBaseline) + conservativeBaseline * 0.55 * shrinkToBaseline);
  meanUtil = clamp(meanUtil - 0.14 * churnPenalty * (1 - signalStrength));
  if (input.cohortState === "non-inferable") meanUtil = clamp(meanUtil * 0.7 + conservativeBaseline * 0.3);
  if (input.cohortState === "churn-dominated") meanUtil = clamp(meanUtil * 0.82 + conservativeBaseline * 0.18);

  let spread = clamp(
    0.18 +
      (1 - inferability) * 0.2 +
      (1 - signalStrength) * 0.1 +
      churnPenalty * 0.08 +
      Math.abs(input.relativePricePosition) * 0.06,
    0.09,
    0.45,
  );
  if (input.cohortState === "churn-dominated") spread = clamp(spread + 0.06, 0.12, 0.5);
  if (input.cohortState === "non-inferable") spread = clamp(spread + 0.12, 0.18, 0.58);

  let downsideSkew = 0.2 + 0.6 * (1 - inferability);
  if (input.cohortState === "churn-dominated") downsideSkew += 0.1;
  if (input.cohortState === "non-inferable") downsideSkew += 0.18;
  const low = clamp(meanUtil - spread * (1 + downsideSkew));
  const high = clamp(meanUtil + spread * (1 - downsideSkew * 0.35));

  return {
    expectedUtilization: meanUtil,
    expectedUtilizationLow: low,
    expectedUtilizationHigh: high,
    pUtilizationAbove25: clamp((meanUtil - 0.12) / 0.88),
    pUtilizationAbove50: clamp((meanUtil - 0.35) / 0.65),
    pUtilizationAbove75: clamp((meanUtil - 0.62) / 0.38),
  };
}

export function estimateExpectedUtilization(input: {
  cohortState: CohortState;
  pressure: number;
  relativePricePosition: number;
  reliabilityScore: number | null;
  machineDepthScore: number;
  concentrationScore: number;
  configVsFamilyHazardDelta: number;
  confidenceScore: number;
  inferabilityScore?: number;
  signalStrengthScore?: number;
  churnScore?: number;
}): UtilizationDistribution;
export function estimateExpectedUtilization(input: {
  disappearedRate: number;
  netSupplyChange: number;
  visibleSupplyCount: number;
  rentableShare: number;
  listingPricePerHour: number;
  medianPrice: number | null;
  reliabilityScore: number | null;
}): number;
export function estimateExpectedUtilization(
  input:
    | {
        cohortState: CohortState;
        pressure: number;
        relativePricePosition: number;
        reliabilityScore: number | null;
        machineDepthScore: number;
        concentrationScore: number;
        configVsFamilyHazardDelta: number;
        confidenceScore: number;
        inferabilityScore?: number;
        signalStrengthScore?: number;
        churnScore?: number;
      }
    | {
        disappearedRate: number;
        netSupplyChange: number;
        visibleSupplyCount: number;
        rentableShare: number;
        listingPricePerHour: number;
        medianPrice: number | null;
        reliabilityScore: number | null;
      },
): UtilizationDistribution | number {
  if ("cohortState" in input) {
    return estimateExpectedUtilizationDistribution(input);
  }
  return estimateExpectedUtilizationLegacy(input);
}

function estimateRoiContextDistribution(input: {
  utilization: UtilizationDistribution;
  listingPricePerHour: number;
  hardwareCost: number;
  powerWatts: number;
  electricityCostPerKwh: number;
  targetPaybackMonths: number;
}): EconomicsDistribution {
  const dailyPowerCost = (input.powerWatts / 1000) * 24 * input.electricityCostPerKwh;

  const revenue = input.utilization.expectedUtilization * input.listingPricePerHour * 24;
  const revenueLow = input.utilization.expectedUtilizationLow * input.listingPricePerHour * 24;
  const revenueHigh = input.utilization.expectedUtilizationHigh * input.listingPricePerHour * 24;

  const margin = revenue - dailyPowerCost;
  const marginLow = revenueLow - dailyPowerCost;
  const marginHigh = revenueHigh - dailyPowerCost;

  const paybackMonths = margin <= 0 ? null : input.hardwareCost / margin / 30.4375;
  const paybackMonthsLow = marginLow <= 0 ? null : input.hardwareCost / marginLow / 30.4375;
  const paybackMonthsHigh = marginHigh <= 0 ? null : input.hardwareCost / marginHigh / 30.4375;

  let pPaybackWithinTarget = 0;
  if (paybackMonths != null) {
    const targetGap = (input.targetPaybackMonths - paybackMonths) / Math.max(input.targetPaybackMonths, 1);
    pPaybackWithinTarget = clamp(logistic(targetGap * 4));
    if (paybackMonthsLow != null && paybackMonthsLow <= input.targetPaybackMonths) {
      pPaybackWithinTarget = Math.max(pPaybackWithinTarget, 0.7);
    }
  }

  return {
    expectedDailyRevenue: revenue,
    expectedDailyRevenueLow: revenueLow,
    expectedDailyRevenueHigh: revenueHigh,
    expectedDailyMargin: margin,
    expectedDailyMarginLow: marginLow,
    expectedDailyMarginHigh: marginHigh,
    expectedPaybackMonths: paybackMonths,
    expectedPaybackMonthsLow: paybackMonthsLow,
    expectedPaybackMonthsHigh: paybackMonthsHigh,
    pPaybackWithinTarget,
  };
}

function estimateRoiContextLegacy(input: {
  expectedUtilizationEstimate: number;
  listingPricePerHour: number;
  hardwareCost: number;
  powerWatts: number;
  electricityCostPerKwh?: number;
}) {
  const costPerKwh = input.electricityCostPerKwh ?? 0.12;
  const expectedDailyRevenue = input.expectedUtilizationEstimate * input.listingPricePerHour * 24;
  const estimatedDailyPowerCost = (input.powerWatts / 1000) * 24 * costPerKwh;
  const estimatedDailyMargin = expectedDailyRevenue - estimatedDailyPowerCost;
  const paybackPeriodDays =
    estimatedDailyMargin <= 0 ? null : input.hardwareCost / estimatedDailyMargin;

  return {
    expectedUtilizationEstimate: input.expectedUtilizationEstimate,
    expectedDailyRevenue,
    estimatedDailyPowerCost,
    estimatedDailyMargin,
    paybackPeriodDays,
  };
}

export function estimateRoiContext(input: {
  utilization: UtilizationDistribution;
  listingPricePerHour: number;
  hardwareCost: number;
  powerWatts: number;
  electricityCostPerKwh: number;
  targetPaybackMonths: number;
}): EconomicsDistribution;
export function estimateRoiContext(input: {
  expectedUtilizationEstimate: number;
  listingPricePerHour: number;
  hardwareCost: number;
  powerWatts: number;
  electricityCostPerKwh?: number;
}): {
  expectedUtilizationEstimate: number;
  expectedDailyRevenue: number;
  estimatedDailyPowerCost: number;
  estimatedDailyMargin: number;
  paybackPeriodDays: number | null;
};
export function estimateRoiContext(
  input:
    | {
        utilization: UtilizationDistribution;
        listingPricePerHour: number;
        hardwareCost: number;
        powerWatts: number;
        electricityCostPerKwh: number;
        targetPaybackMonths: number;
      }
    | {
        expectedUtilizationEstimate: number;
        listingPricePerHour: number;
        hardwareCost: number;
        powerWatts: number;
        electricityCostPerKwh?: number;
      },
) {
  if ("utilization" in input) {
    return estimateRoiContextDistribution(input);
  }
  return estimateRoiContextLegacy(input);
}

function buildRecommendationLegacy(input: {
  regime: Regime;
  lowBandDisappearedRate: number;
  midBandDisappearedRate: number;
  highBandDisappearedRate: number;
  topHostShare: number;
  marketPressureScore: number;
}): Recommendation {
  const cheapClearingOnly =
    input.lowBandDisappearedRate > input.midBandDisappearedRate &&
    input.lowBandDisappearedRate > input.highBandDisappearedRate;
  const premiumClearing = input.highBandDisappearedRate >= input.lowBandDisappearedRate;
  const concentrated = input.topHostShare >= 0.45;

  if (input.regime === "tight" && premiumClearing && !concentrated) {
    return {
      recommendationLabel: "Buy",
      recommendationReasonPrimary:
        "Tightening proxy: disappeared offers elevated with stable-to-rising cohort pricing.",
      recommendationReasonSecondary:
        "Premium and mid-price bands are disappearing, not only the cheapest inventory.",
      recommendationConfidenceNote:
        "Inference-only signal. Disappearance can include reasons other than rental.",
    };
  }

  if (input.regime === "oversupplied") {
    return {
      recommendationLabel: "Avoid",
      recommendationReasonPrimary:
        "Oversupplied proxy: new offers outpacing disappearances while pricing softens.",
      recommendationReasonSecondary: cheapClearingOnly
        ? "Mostly low-priced inventory is clearing; premium clearing support is weak."
        : "Clearing support is weak relative to incoming supply.",
      recommendationConfidenceNote:
        "Treat as market-structure proxy, not a direct demand feed.",
    };
  }

  return {
    recommendationLabel: concentrated ? "Watch" : "Avoid",
    recommendationReasonPrimary:
      input.marketPressureScore >= 55
        ? "Mixed market regime: pressure exists but trend confirmation is incomplete."
        : "Weak pressure and unclear clearing signal for this cohort.",
    recommendationReasonSecondary: concentrated
      ? "Concentrated competition: top host controls a large share of visible supply."
      : "Monitor 24h/7d pressure and price-band clearing before committing hardware.",
    recommendationConfidenceNote:
      "Confidence improves with deeper history and stronger cross-bucket consistency.",
  };
}

function buildRecommendationDistribution(input: {
  pPaybackWithinTarget: number;
  expectedUtilization: number;
  confidenceScore: number;
  cohortState: CohortState;
  concentrationRisk: number;
  downsideRisk: number;
  inferabilityScore?: number;
  identityQualityScore?: number;
  churnScore?: number;
  signalStrengthScore?: number;
}): Recommendation {
  const inferability = input.inferabilityScore ?? input.confidenceScore;
  const identityQuality = input.identityQualityScore ?? 60;
  const churnScore = input.churnScore ?? 40;
  const signalStrength = input.signalStrengthScore ?? 55;

  if (input.cohortState === "non-inferable" || inferability < 35 || identityQuality < 40) {
    return {
      recommendationLabel: "Avoid",
      recommendationReasonPrimary:
        "Market is non-inferable for decision-grade forecasting with current signal quality.",
      recommendationReasonSecondary:
        "Identity quality and persistence evidence are insufficient for a buy-side commitment.",
      recommendationConfidenceNote: "Hard veto applied: uncertainty is too high for strong action.",
      forecastSuppressed: true,
      vetoReason:
        input.cohortState === "non-inferable"
          ? "non_inferable"
          : identityQuality < 40
            ? "identity_quality"
            : "low_inferability",
    };
  }

  if (input.cohortState === "churn-dominated" && (signalStrength < 72 || input.pPaybackWithinTarget < 0.8)) {
    return {
      recommendationLabel: "Speculative",
      recommendationReasonPrimary:
        "High churn with weak persistent contraction suggests movement, not reliable consumption.",
      recommendationReasonSecondary:
        "Use discounted entries and small sizing until persistence-based evidence strengthens.",
      recommendationConfidenceNote: "Strong recommendations are suppressed in churn-dominated regimes.",
      forecastSuppressed: true,
      vetoReason: "churn_dominated",
    };
  }

  if (input.pPaybackWithinTarget >= 0.7 && input.expectedUtilization >= 0.6 && input.confidenceScore >= 65) {
    return {
      recommendationLabel: "Buy",
      recommendationReasonPrimary: "Payback probability and utilization are both strong with supportive cohort pressure.",
      recommendationReasonSecondary: "Observed supply behavior suggests sustained tightness rather than short-lived spikes.",
      recommendationConfidenceNote: "Signals have enough depth for actionable confidence.",
      forecastSuppressed: false,
      vetoReason: null,
    };
  }

  if (input.pPaybackWithinTarget >= 0.55 && input.expectedUtilization >= 0.5 && input.confidenceScore >= 50) {
    return {
      recommendationLabel: "Buy if discounted",
      recommendationReasonPrimary: "Economics are close to target but leave limited downside buffer.",
      recommendationReasonSecondary: "A lower entry cost materially improves payback odds under uncertainty.",
      recommendationConfidenceNote: "Moderate confidence; verify current pressure persistence before committing.",
      forecastSuppressed: false,
      vetoReason: null,
    };
  }

  if (input.confidenceScore < 45 || inferability < 45 || input.cohortState === "thin-data") {
    return {
      recommendationLabel: "Speculative",
      recommendationReasonPrimary: "Signal depth is thin relative to the decision size.",
      recommendationReasonSecondary: "Use small position sizing until lifecycle and persistence evidence deepens.",
      recommendationConfidenceNote: "Low confidence regime, forecasts are heavily shrunk to family baseline.",
      forecastSuppressed: true,
      vetoReason: "low_inferability",
    };
  }

  if (input.downsideRisk > 0.6 || input.concentrationRisk > 0.55 || input.cohortState === "oversupplied") {
    return {
      recommendationLabel: "Avoid",
      recommendationReasonPrimary: "Downside risk and oversupply/fragility indicators outweigh expected upside.",
      recommendationReasonSecondary: "Observed visible supply behavior does not support stable consumption at target economics.",
      recommendationConfidenceNote: "Confidence may be adequate, but evidence is consistently unfavorable.",
      forecastSuppressed: false,
      vetoReason: null,
    };
  }

  return {
    recommendationLabel: "Watch",
    recommendationReasonPrimary: "Setup is mixed: not decisively attractive or decisively impaired.",
    recommendationReasonSecondary: "Track 24h/72h persistence and concentration shifts before entering.",
    recommendationConfidenceNote: "Wait for stronger probability separation or better pricing.",
    forecastSuppressed: false,
    vetoReason: null,
  };
}

export function buildRecommendation(
  input:
    | {
        pPaybackWithinTarget: number;
        expectedUtilization: number;
        confidenceScore: number;
        cohortState: CohortState;
        concentrationRisk: number;
        downsideRisk: number;
        inferabilityScore?: number;
        identityQualityScore?: number;
        churnScore?: number;
        signalStrengthScore?: number;
      }
    | {
        regime: Regime;
        lowBandDisappearedRate: number;
        midBandDisappearedRate: number;
        highBandDisappearedRate: number;
        topHostShare: number;
        marketPressureScore: number;
      },
): Recommendation {
  if ("pPaybackWithinTarget" in input) {
    return buildRecommendationDistribution(input);
  }
  return buildRecommendationLegacy(input);
}

export function brierScore(items: Array<{ predicted: number; realized: boolean }>): number {
  if (items.length === 0) return 0;
  return mean(items.map((item) => (item.predicted - (item.realized ? 1 : 0)) ** 2));
}

export type CalibrationBucket = {
  bucket: string;
  count: number;
  avgPredicted: number;
  realizedRate: number;
};

export function buildCalibrationBuckets(
  items: Array<{ predicted: number; realized: boolean }>,
  step = 0.1,
): CalibrationBucket[] {
  const bucketMap = new Map<string, Array<{ predicted: number; realized: boolean }>>();

  for (const item of items) {
    const lower = Math.floor(clamp(item.predicted) / step) * step;
    const upper = Math.min(1, lower + step);
    const key = `${lower.toFixed(1)}-${upper.toFixed(1)}`;
    const existing = bucketMap.get(key) ?? [];
    existing.push(item);
    bucketMap.set(key, existing);
  }

  return [...bucketMap.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([bucket, values]) => ({
      bucket,
      count: values.length,
      avgPredicted: mean(values.map((value) => value.predicted)),
      realizedRate: mean(values.map((value) => (value.realized ? 1 : 0))),
    }));
}

export function computeNoiseScore(points: TrendPoint[]): number {
  if (points.length <= 1) return 80;
  const disappears = points.map((point) => point.disappearedRate ?? 0);
  const prices = points.map((point) => point.medianPrice ?? 0);
  const reappears = points.map((point) => point.reappearedRate ?? 0);
  const temporaryMissing = points.map((point) => point.temporaryMissingRate ?? 0);
  const weakIdentity = points.map((point) => 1 - (point.identityQualityScore ?? 60) / 100);

  const raw = clamp(
    stddev(disappears) * 2.1 +
      stddev(prices) +
      mean(reappears) * 1.1 +
      mean(temporaryMissing) * 1.15 +
      mean(weakIdentity) * 0.8,
    0,
    1.2,
  );
  return raw * 100;
}

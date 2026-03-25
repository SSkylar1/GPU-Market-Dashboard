import { subDays, subHours } from "date-fns";

export const MARKET_INTELLIGENCE_CONFIG = {
  pressureWeights: {
    churn: 0.35,
    supply: 0.25,
    price: 0.2,
    availability: 0.2,
  },
  regimeThresholds: {
    tightDisappearedRate: 0.22,
    tightPressure: 62,
    oversupplyNewRate: 0.22,
    oversupplyPressure: 40,
  },
  roiDefaults: {
    electricityCostPerKwh: 0.12,
  },
} as const;

export type TrendPoint = {
  bucketStartUtc: Date;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers?: number;
  minPrice?: number | null;
  p10Price?: number | null;
  medianPrice?: number | null;
  p90Price?: number | null;
  newOfferCount?: number | null;
  disappearedOfferCount?: number | null;
  newOfferRate?: number | null;
  disappearedRate?: number | null;
  netSupplyChange?: number | null;
  medianPriceChange?: number | null;
  rentableShareChange?: number | null;
  marketPressureScore?: number | null;
  marketPressurePriceComponent?: number | null;
  marketPressureChurnComponent?: number | null;
  marketPressureSupplyComponent?: number | null;
  marketPressureAvailabilityComponent?: number | null;
  lowBandDisappearedCount?: number | null;
  midBandDisappearedCount?: number | null;
  highBandDisappearedCount?: number | null;
  lowBandDisappearedRate?: number | null;
  midBandDisappearedRate?: number | null;
  highBandDisappearedRate?: number | null;
};

export type CompetitionMetrics = {
  distinctHosts: number;
  distinctMachines: number;
  offersPerHost: number;
  offersPerMachine: number;
  topHostShare: number;
  top5HostShare: number;
  hostConcentrationIndex: number;
  avgReliabilityScore: number | null;
};

export type RoiEstimate = {
  expectedUtilizationEstimate: number;
  expectedDailyRevenue: number;
  estimatedDailyPowerCost: number;
  estimatedDailyMargin: number;
  paybackPeriodDays: number | null;
};

export type Regime = "tight" | "balanced" | "oversupplied";

export type Recommendation = {
  recommendationLabel: "Buy" | "Wait" | "Avoid" | "Race-to-bottom risk";
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
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
  disappearedRate: MetricTrend | null;
  newOfferRate: MetricTrend | null;
  medianPrice: MetricTrend | null;
  rentableShare: MetricTrend | null;
  marketPressureScore: MetricTrend | null;
};

function clamp(value: number, min = 0, max = 1): number {
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

  // Normalize each component to 0..1 where 1 means tighter market proxy.
  const churnComponent = clamp(input.disappearedRate);
  const supplyComponent = clamp(1 - input.newOfferRate);
  const priceComponent = clamp((priceChangePct + 0.2) / 0.4);
  const availabilityComponent = clamp((-input.rentableShareChange + 0.25) / 0.5);

  const score01 =
    churnComponent * MARKET_INTELLIGENCE_CONFIG.pressureWeights.churn +
    supplyComponent * MARKET_INTELLIGENCE_CONFIG.pressureWeights.supply +
    priceComponent * MARKET_INTELLIGENCE_CONFIG.pressureWeights.price +
    availabilityComponent * MARKET_INTELLIGENCE_CONFIG.pressureWeights.availability;

  return {
    marketPressureScore: score01 * 100,
    marketPressureChurnComponent: churnComponent * 100,
    marketPressureSupplyComponent: supplyComponent * 100,
    marketPressurePriceComponent: priceComponent * 100,
    marketPressureAvailabilityComponent: availabilityComponent * 100,
  };
}

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function slope(values: number[]): number {
  if (values.length < 2) return 0;
  const n = values.length;
  const xs = Array.from({ length: n }, (_, i) => i);
  const xMean = mean(xs);
  const yMean = mean(values);
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = xs[i] - xMean;
    num += dx * (values[i] - yMean);
    den += dx * dx;
  }
  return den === 0 ? 0 : num / den;
}

function buildMetricTrend(values: number[]): MetricTrend | null {
  if (values.length === 0) return null;
  const latest = values[values.length - 1];
  const earliest = values[0];
  const trailingAverage = mean(values);
  const absoluteChange = latest - earliest;
  const percentChange = Math.abs(earliest) < 1e-9 ? null : (absoluteChange / earliest) * 100;
  const slopePerBucket = slope(values);
  const direction =
    Math.abs(slopePerBucket) < 1e-6 ? "flat" : slopePerBucket > 0 ? "up" : "down";

  return {
    latest,
    trailingAverage,
    absoluteChange,
    percentChange,
    slopePerBucket,
    direction,
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

  const supplyValues = filtered.map((point) => point.totalOffers);
  const disappearedValues = filtered.map((point) => point.disappearedRate ?? 0);
  const newValues = filtered.map((point) => point.newOfferRate ?? 0);
  const medianValues = filtered.map((point) => point.medianPrice ?? 0);
  const rentableShareValues = filtered.map((point) => safeDiv(point.rentableOffers, point.totalOffers));
  const pressureValues = filtered.map((point) => point.marketPressureScore ?? 0);

  return {
    pointCount: filtered.length,
    supply: buildMetricTrend(supplyValues),
    disappearedRate: buildMetricTrend(disappearedValues),
    newOfferRate: buildMetricTrend(newValues),
    medianPrice: buildMetricTrend(medianValues),
    rentableShare: buildMetricTrend(rentableShareValues),
    marketPressureScore: buildMetricTrend(pressureValues),
  };
}

export function classifyMarketRegime(input: {
  disappearedRate: number;
  newOfferRate: number;
  netSupplyChange: number;
  medianPriceChange: number;
  rentableShareChange: number;
  marketPressureScore: number;
}): Regime {
  const isTight =
    input.disappearedRate >= MARKET_INTELLIGENCE_CONFIG.regimeThresholds.tightDisappearedRate &&
    input.netSupplyChange <= 0 &&
    input.medianPriceChange >= 0 &&
    input.rentableShareChange <= 0 &&
    input.marketPressureScore >= MARKET_INTELLIGENCE_CONFIG.regimeThresholds.tightPressure;

  if (isTight) return "tight";

  const isOversupplied =
    input.newOfferRate >= MARKET_INTELLIGENCE_CONFIG.regimeThresholds.oversupplyNewRate &&
    input.netSupplyChange > 0 &&
    input.medianPriceChange <= 0 &&
    input.rentableShareChange >= 0 &&
    input.marketPressureScore <= MARKET_INTELLIGENCE_CONFIG.regimeThresholds.oversupplyPressure;

  if (isOversupplied) return "oversupplied";
  return "balanced";
}

export function buildRecommendation(input: {
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
      recommendationLabel: "Race-to-bottom risk",
      recommendationReasonPrimary:
        "Oversupplied proxy: new offers outpacing disappearances while pricing softens.",
      recommendationReasonSecondary:
        cheapClearingOnly
          ? "Mostly low-priced inventory is clearing; premium clearing support is weak."
          : "Clearing support is weak relative to incoming supply.",
      recommendationConfidenceNote:
        "Treat as market-structure proxy, not a direct demand feed.",
    };
  }

  return {
    recommendationLabel: concentrated ? "Wait" : "Avoid",
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

export function estimateExpectedUtilization(input: {
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
    input.reliabilityScore == null
      ? 0.5
      : clamp((input.reliabilityScore - 0.9) / 0.1);

  const estimate =
    0.1 +
    churnSignal * 0.28 +
    supplySignal * 0.2 +
    availabilitySignal * 0.22 +
    priceSignal * 0.2 +
    reliabilitySignal * 0.1;

  return clamp(estimate, 0, 0.98);
}

export function estimateRoiContext(input: {
  expectedUtilizationEstimate: number;
  listingPricePerHour: number;
  hardwareCost: number;
  powerWatts: number;
  electricityCostPerKwh?: number;
}): RoiEstimate {
  const costPerKwh =
    input.electricityCostPerKwh ?? MARKET_INTELLIGENCE_CONFIG.roiDefaults.electricityCostPerKwh;

  const expectedDailyRevenue = input.expectedUtilizationEstimate * input.listingPricePerHour * 24;
  const estimatedDailyPowerCost = (input.powerWatts / 1000) * 24 * costPerKwh;
  const estimatedDailyMargin = expectedDailyRevenue - estimatedDailyPowerCost;

  const paybackPeriodDays = estimatedDailyMargin <= 0 ? null : input.hardwareCost / estimatedDailyMargin;

  return {
    expectedUtilizationEstimate: input.expectedUtilizationEstimate,
    expectedDailyRevenue,
    estimatedDailyPowerCost,
    estimatedDailyMargin,
    paybackPeriodDays,
  };
}

export function computeCompetitionMetrics(offers: Array<{
  hostId: number | null;
  machineId: number | null;
  reliabilityScore: number | null;
}>): CompetitionMetrics {
  const hostCounts = new Map<number, number>();
  const machineIds = new Set<number>();
  const reliability: number[] = [];

  for (const offer of offers) {
    if (offer.hostId != null) {
      hostCounts.set(offer.hostId, (hostCounts.get(offer.hostId) ?? 0) + 1);
    }
    if (offer.machineId != null) {
      machineIds.add(offer.machineId);
    }
    if (offer.reliabilityScore != null && Number.isFinite(offer.reliabilityScore)) {
      reliability.push(offer.reliabilityScore);
    }
  }

  const totalOffers = offers.length;
  const sortedHostCounts = [...hostCounts.values()].sort((a, b) => b - a);
  const topHost = sortedHostCounts[0] ?? 0;
  const top5 = sortedHostCounts.slice(0, 5).reduce((acc, value) => acc + value, 0);
  const hhi = sortedHostCounts.reduce((acc, count) => {
    const share = safeDiv(count, Math.max(totalOffers, 1));
    return acc + share * share;
  }, 0);

  return {
    distinctHosts: hostCounts.size,
    distinctMachines: machineIds.size,
    offersPerHost: safeDiv(totalOffers, Math.max(hostCounts.size, 1)),
    offersPerMachine: safeDiv(totalOffers, Math.max(machineIds.size, 1)),
    topHostShare: safeDiv(topHost, Math.max(totalOffers, 1)),
    top5HostShare: safeDiv(top5, Math.max(totalOffers, 1)),
    hostConcentrationIndex: hhi,
    avgReliabilityScore: reliability.length === 0 ? null : mean(reliability),
  };
}

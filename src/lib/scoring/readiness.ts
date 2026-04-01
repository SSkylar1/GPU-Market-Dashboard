import { clamp } from "@/lib/metrics/intelligence";

export type RecommendationLabel = "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy";

export type ReadinessBand = "Too early" | "Emerging signal" | "Usable with caution" | "Decision-grade";

export type ReadinessThresholds = {
  tooEarlyMax: number;
  emergingMax: number;
  usableMax: number;
  buyInferabilityMin: number;
  buyConfidenceMin: number;
  buySignalMin: number;
  buyPriceAdvantageMin: number;
  discountedPriceAdvantageMin: number;
  watchInferabilityMin: number;
  speculativeInferabilityMin: number;
  churnRiskMax: number;
};

export const DEFAULT_READINESS_THRESHOLDS: ReadinessThresholds = {
  tooEarlyMax: 34,
  emergingMax: 54,
  usableMax: 74,
  buyInferabilityMin: 68,
  buyConfidenceMin: 64,
  buySignalMin: 62,
  buyPriceAdvantageMin: 0.08,
  discountedPriceAdvantageMin: 0.03,
  watchInferabilityMin: 45,
  speculativeInferabilityMin: 35,
  churnRiskMax: 56,
};

export type ObservationMetrics = {
  observationCount: number;
  observationsPerOffer: number;
  medianPollGapMinutes: number;
  maxPollGapMinutes: number;
  coverageRatio: number;
  offerSeenSpanMinutes: number;
  cohortObservationDensityScore: number;
  labelabilityScore: number;
  futureWindowCoverage12h: number;
  futureWindowCoverage24h: number;
  futureWindowCoverage72h: number;
  samplingQualityScore: number;
  lifecycleObservabilityScore: number;
  insufficientSampling: boolean;
};

export type ReadinessInput = {
  inferabilityScore: number;
  confidenceScore: number;
  identityQualityScore: number;
  timeDepthScore: number;
  crossSectionDepthScore: number;
  dataDepthScore: number;
  signalStrengthScore: number;
  churnScore: number;
  machineBreadth: number;
  historyContinuity: number;
  observation: ObservationMetrics;
  state: string;
};

export type ReadinessResult = {
  readinessScore: number;
  readinessBand: ReadinessBand;
  readinessBreakdown: Record<string, number>;
  graduationTags: string[];
};

export type InferabilityDecomposition = {
  marketAmbiguity: number;
  poorSampling: number;
  weakIdentity: number;
  thinDepth: number;
  churnReappearance: number;
};

export function computeSamplingQualityScore(input: {
  observationCount: number;
  observationsPerOffer: number;
  medianPollGapMinutes: number;
  maxPollGapMinutes: number;
  coverageRatio: number;
  futureWindowCoverage24h: number;
}): number {
  const observationDepth = clamp(input.observationCount / 80);
  const offerDepth = clamp(input.observationsPerOffer / 8);
  const medianGap = 1 - clamp((input.medianPollGapMinutes - 2) / 18);
  const maxGap = 1 - clamp((input.maxPollGapMinutes - 5) / 55);
  return (
    100 *
    clamp(
      0.24 * observationDepth +
        0.2 * offerDepth +
        0.2 * medianGap +
        0.12 * maxGap +
        0.14 * clamp(input.coverageRatio) +
        0.1 * clamp(input.futureWindowCoverage24h),
    )
  );
}

export function computeLifecycleObservabilityScore(input: {
  labelabilityScore: number;
  offerSeenSpanMinutes: number;
  futureWindowCoverage72h: number;
  historyContinuity: number;
}): number {
  const spanScore = clamp(input.offerSeenSpanMinutes / (72 * 60));
  return (
    100 *
    clamp(
      0.34 * clamp(input.labelabilityScore / 100) +
        0.26 * spanScore +
        0.2 * clamp(input.futureWindowCoverage72h) +
        0.2 * clamp(input.historyContinuity / 100),
    )
  );
}

export function computeReadiness(input: ReadinessInput): ReadinessResult {
  const churnPenalty = clamp(input.churnScore / 100);
  const thinPenalty = input.state === "thin-data" ? 0.12 : 0;
  const nonInferablePenalty = input.state === "non-inferable" ? 0.2 : 0;
  const churnDominatedPenalty = input.state === "churn-dominated" ? 0.15 : 0;
  const insufficientSamplingPenalty = input.observation.insufficientSampling ? 0.16 : 0;

  const score =
    100 *
    clamp(
      0.2 * clamp(input.inferabilityScore / 100) +
        0.14 * clamp(input.confidenceScore / 100) +
        0.12 * clamp(input.identityQualityScore / 100) +
        0.1 * clamp(input.timeDepthScore / 100) +
        0.09 * clamp(input.crossSectionDepthScore / 100) +
        0.08 * clamp(input.dataDepthScore / 100) +
        0.1 * clamp(input.observation.samplingQualityScore / 100) +
        0.09 * clamp(input.observation.lifecycleObservabilityScore / 100) +
        0.08 * clamp(input.machineBreadth / 100) +
        0.08 * clamp(input.historyContinuity / 100) -
        0.18 * churnPenalty -
        thinPenalty -
        nonInferablePenalty -
        churnDominatedPenalty -
        insufficientSamplingPenalty,
    );

  let readinessBand: ReadinessBand = "Decision-grade";
  if (score <= DEFAULT_READINESS_THRESHOLDS.tooEarlyMax) readinessBand = "Too early";
  else if (score <= DEFAULT_READINESS_THRESHOLDS.emergingMax) readinessBand = "Emerging signal";
  else if (score <= DEFAULT_READINESS_THRESHOLDS.usableMax) readinessBand = "Usable with caution";

  const tags: string[] = [];
  if (score >= 60 && score < 75) tags.push("Near usable");
  if (score >= 35 && score < 55) tags.push("Emerging");
  if (input.timeDepthScore < 45 || input.historyContinuity < 45) tags.push("Needs more history");
  if (input.identityQualityScore < 50) tags.push("Identity issue");
  if (input.observation.insufficientSampling || input.observation.samplingQualityScore < 45)
    tags.push("Under-sampled");
  if (input.churnScore >= 58) tags.push("Churn-heavy");
  if (score >= 72 && readinessBand !== "Decision-grade") tags.push("Graduating soon");
  if (input.inferabilityScore < 45 || input.confidenceScore < 45) tags.push("Confidence issue");

  return {
    readinessScore: Number(score.toFixed(2)),
    readinessBand,
    readinessBreakdown: {
      inferability: Number((0.2 * clamp(input.inferabilityScore / 100) * 100).toFixed(2)),
      confidence: Number((0.14 * clamp(input.confidenceScore / 100) * 100).toFixed(2)),
      identity: Number((0.12 * clamp(input.identityQualityScore / 100) * 100).toFixed(2)),
      depth: Number((0.27 * clamp((input.timeDepthScore + input.crossSectionDepthScore + input.dataDepthScore) / 300) * 100).toFixed(2)),
      sampling: Number((0.1 * clamp(input.observation.samplingQualityScore / 100) * 100).toFixed(2)),
      lifecycle: Number((0.09 * clamp(input.observation.lifecycleObservabilityScore / 100) * 100).toFixed(2)),
      churnPenalty: Number((0.18 * churnPenalty * 100).toFixed(2)),
    },
    graduationTags: Array.from(new Set(tags)),
  };
}

export function decomposeInferability(input: {
  inferabilityScore: number;
  samplingQualityScore: number;
  identityQualityScore: number;
  dataDepthScore: number;
  churnScore: number;
}): InferabilityDecomposition {
  const marketAmbiguity = clamp((100 - input.inferabilityScore) / 100);
  const poorSampling = clamp((100 - input.samplingQualityScore) / 100);
  const weakIdentity = clamp((100 - input.identityQualityScore) / 100);
  const thinDepth = clamp((100 - input.dataDepthScore) / 100);
  const churnReappearance = clamp(input.churnScore / 100);
  const total = marketAmbiguity + poorSampling + weakIdentity + thinDepth + churnReappearance || 1;

  return {
    marketAmbiguity: Number((marketAmbiguity / total).toFixed(3)),
    poorSampling: Number((poorSampling / total).toFixed(3)),
    weakIdentity: Number((weakIdentity / total).toFixed(3)),
    thinDepth: Number((thinDepth / total).toFixed(3)),
    churnReappearance: Number((churnReappearance / total).toFixed(3)),
  };
}

export function buildTransitionGuidance(input: {
  recommendation: RecommendationLabel;
  inferabilityScore: number;
  confidenceScore: number;
  signalStrengthScore: number;
  readinessScore: number;
  priceAdvantage: number;
  churnScore: number;
  thresholds?: ReadinessThresholds;
}): {
  nearestUpgrade: RecommendationLabel | null;
  nearestDowngrade: RecommendationLabel | null;
  upgradeGuidance: string[];
  downgradeRiskFactors: string[];
} {
  const t = input.thresholds ?? DEFAULT_READINESS_THRESHOLDS;
  const levels: RecommendationLabel[] = ["Avoid", "Watch", "Speculative", "Buy if discounted", "Buy"];
  const idx = levels.indexOf(input.recommendation);
  const nearestUpgrade = idx < levels.length - 1 ? levels[idx + 1] : null;
  const nearestDowngrade = idx > 0 ? levels[idx - 1] : null;
  const upgradeGuidance: string[] = [];
  const downgradeRiskFactors: string[] = [];

  if (input.inferabilityScore < t.watchInferabilityMin) {
    upgradeGuidance.push(`Inferability needs to exceed ${t.watchInferabilityMin}.`);
  }
  if (input.readinessScore < t.emergingMax) {
    upgradeGuidance.push(`Readiness needs to exceed ${t.emergingMax + 1}.`);
  }
  if (input.signalStrengthScore < t.buySignalMin) {
    upgradeGuidance.push(`Signal strength needs to exceed ${t.buySignalMin}.`);
  }
  if (input.confidenceScore < t.buyConfidenceMin) {
    upgradeGuidance.push(`Confidence needs to exceed ${t.buyConfidenceMin}.`);
  }
  if (input.priceAdvantage < t.discountedPriceAdvantageMin) {
    upgradeGuidance.push(
      `Price position should improve by at least ${(t.discountedPriceAdvantageMin * 100).toFixed(1)}%.`,
    );
  }

  if (input.churnScore > t.churnRiskMax) {
    downgradeRiskFactors.push(`Churn is above ${(t.churnRiskMax).toFixed(0)} and may force downgrade.`);
  }
  if (input.inferabilityScore < t.speculativeInferabilityMin) {
    downgradeRiskFactors.push(`Inferability under ${t.speculativeInferabilityMin} risks hard suppression.`);
  }
  if (input.priceAdvantage < -0.04) {
    downgradeRiskFactors.push("Premium pricing vs cohort median increases downgrade risk.");
  }

  return {
    nearestUpgrade,
    nearestDowngrade,
    upgradeGuidance,
    downgradeRiskFactors,
  };
}

export function computeExploratoryOpportunityScore(input: {
  pressure: number;
  readinessScore: number;
  inferabilityScore: number;
  confidenceScore: number;
  consumption24h: number;
  priceAdvantage: number;
  churnScore: number;
  samplingQualityScore: number;
  identityQualityScore: number;
}): number {
  return Number(
    (
      0.18 * clamp(input.pressure / 100) +
      0.18 * clamp(input.readinessScore / 100) +
      0.13 * clamp(input.inferabilityScore / 100) +
      0.1 * clamp(input.confidenceScore / 100) +
      0.16 * clamp(input.consumption24h) +
      0.11 * clamp(0.5 + input.priceAdvantage) +
      0.08 * clamp(input.samplingQualityScore / 100) +
      0.06 * clamp(input.identityQualityScore / 100) -
      0.18 * clamp(input.churnScore / 100)
    ).toFixed(4),
  );
}

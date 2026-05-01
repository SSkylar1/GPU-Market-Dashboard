import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { PrismaClient, type Offer } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { floorToUtcHalfHour } from "../src/lib/metrics/aggregation";
import { classifyDisappearanceOutcome, summarizeReappearanceGaps } from "../src/lib/metrics/transitions";
import {
  buildEmpiricalCalibrator,
  clamp,
  combineDepthScores,
  classifyCohortState,
  computeCohortPressureScore,
  computeNoiseScore,
  computeStateConfidence,
  estimateConsumptionProbability,
  forecastProbabilitiesFromState,
  mean,
  percentile,
  safeDiv,
  shrinkTowardsFamily,
  type CohortState,
  type TrendPoint,
} from "../src/lib/metrics/intelligence";
import { buildOfferIdentity, type OfferIdentityResult } from "../src/lib/metrics/offerIdentity";
import {
  buildConsumptionEventLabel,
  CONSUMPTION_HORIZONS,
  type ConsumptionEventLabel,
  type ConsumptionHorizon,
} from "../src/lib/metrics/consumptionLabels";

const PERSISTENCE_BUCKETS = Number(process.env.PERSISTENCE_BUCKETS ?? 3);
const SHORT_GAP_MAX_BUCKETS = Number(process.env.SHORT_GAP_MAX_BUCKETS ?? 2);
const MODEL_VERSION = "predictive-v3.2";
const CALIBRATION_VERSION = "consumption-cal-v2";
const CALIBRATION_BUCKET_STEP = 0.1;
const CALIBRATION_DIR = "docs/artifacts";
const CALIBRATION_FILE = `${CALIBRATION_DIR}/consumption-calibration-v2.json`;
const VALIDATION_FILE = `${CALIBRATION_DIR}/validation-scorecard-v32.json`;

type CohortKey = {
  source: string;
  gpuName: string;
  numGpus: number | null;
  offerType: string | null;
};

type CohortBucket = {
  key: CohortKey;
  bucketStartUtc: Date;
  snapshotId: string;
  snapshotCount: number;
  totalOffers: number;
  uniqueMachines: number;
  uniqueHosts: number;
  rentableOffers: number;
  rentedOffers: number;
  continuingOffers: number;
  newOffers: number;
  disappearedOffers: number;
  reappearedOffers: number;
  persistentDisappearedOffers: number;
  newOfferRate: number;
  disappearedRate: number;
  reappearedRate: number;
  temporaryMissingRate: number;
  reappearedShortGapRate: number;
  reappearedLongGapRate: number;
  medianReappearanceDelayBuckets: number | null;
  persistentDisappearanceRate: number;
  persistentDisappearanceRateN: number;
  churnAdjustedDisappearanceRate: number;
  machineEntryRate: number;
  machineExitRate: number;
  hostEntryRate: number;
  hostExitRate: number;
  impliedUtilization: number;
  availabilityRatio: number;
  minPrice: number | null;
  p10Price: number | null;
  medianPrice: number | null;
  p90Price: number | null;
  maxPrice: number | null;
  priceCv: number | null;
  medianPriceChange: number | null;
  lowBandDisappearedRate: number;
  midBandDisappearedRate: number;
  highBandDisappearedRate: number;
  lowBandPersistentDisappearedRate: number;
  midBandPersistentDisappearedRate: number;
  highBandPersistentDisappearedRate: number;
  supplyTightnessScore: number;
  movementScore: number;
  machineDepthScore: number;
  concentrationScore: number;
  cohortPressureScore: number;
  pressureAcceleration: number;
  pressurePersistence: number;
  state: CohortState;
  stateConfidence: number;
  timeDepthScore: number;
  crossSectionDepthScore: number;
  dataDepthScore: number;
  noiseScore: number;
  churnScore: number;
  signalStrengthScore: number;
  inferabilityScore: number;
  identityQualityScore: number;
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
  configVsFamilyPressureDelta: number | null;
  configVsFamilyPriceDelta: number | null;
  configVsFamilyHazardDelta: number | null;
  machineConcentrationShareTop1: number;
  machineConcentrationShareTop3: number;
  hostConcentrationShareTop1: number;
  hostConcentrationShareTop3: number;
  machinePersistenceRate: number;
  hostPersistenceRate: number;
  newMachineEntryRate: number;
  disappearingMachineRate: number;
  consumptionEventLabels: Array<ConsumptionEventLabel & { stableOfferFingerprint: string }>;
  consumptionLabelSummaryByHorizon: Record<ConsumptionHorizon, ConsumptionLabelSummary>;
};

type ConsumptionLabelSummary = {
  usableCount: number;
  censoredCount: number;
  consumedCount: number;
  realizedRate: number | null;
};

type LifecycleWorking = {
  source: string;
  offerFingerprint: string;
  stableOfferFingerprint: string;
  latestVersionFingerprint: string;
  identityStrategy: string;
  identityQualityScore: number;
  offerExternalId: string | null;
  gpuName: string;
  numGpus: number;
  offerType: string;
  machineId: number | null;
  hostId: number | null;
  firstSeenAt: Date;
  lastSeenAt: Date;
  totalVisibleSnapshots: number;
  seenCount: number;
  totalVisibleHours: number;
  cumulativeVisibleMinutes: number;
  offerSeenSpanMinutes: number;
  disappearanceCount: number;
  reappearanceCount: number;
  firstMissingAt: Date | null;
  reappearedAt: Date | null;
  gapDurationMinutes: number | null;
  visibilitySegmentCount: number;
  longestContinuousVisibleHours: number;
  estimatedConsumedAt: Date | null;
  insufficientObservation: boolean;
  latestKnownPricePerHour: number | null;
  latestKnownReliabilityScore: number | null;
  firstKnownPricePerHour: number | null;
  minObservedPricePerHour: number | null;
  maxObservedPricePerHour: number | null;
  priceEditCount: number;
  mutationCount: number;
  lastStatus: "active" | "disappeared" | "reappeared" | "stale";
  lastSeenSnapshotIndex: number;
  currentSegmentStart: Date;
  currentSegmentPrices: number[];
  currentSegmentStartPrice: number | null;
  currentSegmentStartRentable: boolean | null;
  priorPrice: number | null;
  reappearanceGapBuckets: number[];
  segments: Array<{
    segmentStartAt: Date;
    segmentEndAt: Date | null;
    durationHours: number | null;
    endedBy: "disappeared" | "still_active" | "unknown";
    startPricePerHour: number | null;
    endPricePerHour: number | null;
    medianPricePerHour: number | null;
    startRentable: boolean | null;
    endRentable: boolean | null;
  }>;
};

type OfferIdentityResolved = Pick<
  OfferIdentityResult,
  "stableOfferFingerprint" | "versionFingerprint" | "strategy" | "identityQualityScore" | "offerExternalId"
>;

function normalizeIdentityStrategy(
  strategy: string | null | undefined,
): OfferIdentityResult["strategy"] {
  if (strategy === "external_id") return strategy;
  if (strategy === "machine_signature") return strategy;
  if (strategy === "host_signature") return strategy;
  return "weak_signature";
}

function bucketizeScore(score: number): "low" | "medium" | "high" {
  if (score >= 70) return "high";
  if (score >= 45) return "medium";
  return "low";
}

function toMinutes(hours: number): number {
  return hours * 60;
}

function summarizePollGapsMinutes(snapshots: Array<{ capturedAt: Date }>): {
  medianPollGapMinutes: number;
  maxPollGapMinutes: number;
} {
  if (snapshots.length < 2) {
    return { medianPollGapMinutes: 30, maxPollGapMinutes: 30 };
  }
  const gaps: number[] = [];
  for (let i = 1; i < snapshots.length; i += 1) {
    const deltaMinutes = (snapshots[i].capturedAt.getTime() - snapshots[i - 1].capturedAt.getTime()) / 60000;
    if (deltaMinutes > 0 && Number.isFinite(deltaMinutes)) gaps.push(deltaMinutes);
  }
  const recentGaps = gaps.slice(-288);
  const qualityGaps = recentGaps.length > 0 ? recentGaps : gaps;
  const medianPollGapMinutes = percentile(qualityGaps, 0.5) ?? 30;
  const maxPollGapMinutes = percentile(qualityGaps, 0.95) ?? medianPollGapMinutes;
  return { medianPollGapMinutes, maxPollGapMinutes };
}

function computeSamplingQualityScore(input: {
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

function computeLifecycleObservabilityScore(input: {
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

function makeCohortKey(key: CohortKey): string {
  return `${key.source}::${key.gpuName}::${key.numGpus == null ? "combined" : key.numGpus}::${key.offerType == null ? "combined" : key.offerType}`;
}

function inferOfferIdentity(offer: Offer, source: string): OfferIdentityResolved {
  const storedStrategy = normalizeIdentityStrategy(offer.identityStrategy);
  if (
    offer.stableOfferFingerprint &&
    offer.stableOfferFingerprint.trim().length > 0 &&
    offer.versionFingerprint &&
    offer.versionFingerprint.trim().length > 0 &&
    !(source.toLowerCase().includes("vast") && storedStrategy === "external_id" && offer.machineId != null)
  ) {
    return {
      stableOfferFingerprint: offer.stableOfferFingerprint,
      versionFingerprint: offer.versionFingerprint,
      strategy: storedStrategy,
      identityQualityScore: offer.identityQualityScore ?? 0.45,
      offerExternalId: offer.offerExternalId,
    };
  }

  const inferred = buildOfferIdentity({
    source,
    offerExternalId: offer.offerExternalId ?? offer.offerId,
    machineId: offer.machineId,
    hostId: offer.hostId,
    gpuName: offer.gpuName,
    numGpus: offer.numGpus,
    offerType: offer.offerType,
    gpuRamGb: offer.gpuRamGb,
    cpuCores: offer.cpuCores,
    ramGb: offer.ramGb,
    diskGb: offer.diskGb,
    reliabilityScore: offer.reliabilityScore,
    verified: offer.verified,
    pricePerHour: offer.pricePerHour,
    inetDownMbps: offer.inetDownMbps,
    inetUpMbps: offer.inetUpMbps,
    geolocation: offer.geolocation,
    sourceFingerprint:
      offer.hostId == null || offer.machineId == null ? null : `${offer.hostId}:${offer.machineId}`,
  });

  return {
    stableOfferFingerprint: inferred.stableOfferFingerprint,
    versionFingerprint: inferred.versionFingerprint,
    strategy: inferred.strategy,
    identityQualityScore: inferred.identityQualityScore,
    offerExternalId: inferred.offerExternalId,
  };
}

function estimateSourceBucketHours(snapshots: Array<{ capturedAt: Date }>): number {
  if (snapshots.length < 2) return 0.5;
  const deltas: number[] = [];
  for (let i = 1; i < snapshots.length; i += 1) {
    const deltaHours = (snapshots[i].capturedAt.getTime() - snapshots[i - 1].capturedAt.getTime()) / 3600000;
    if (deltaHours > 0 && Number.isFinite(deltaHours)) deltas.push(deltaHours);
  }
  const medianDelta = percentile(deltas, 0.5);
  return medianDelta == null || medianDelta <= 0 ? 0.5 : medianDelta;
}

function findNextAppearanceIndex(indices: number[], afterIndex: number): number | null {
  let left = 0;
  let right = indices.length - 1;
  let candidate: number | null = null;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const value = indices[mid];
    if (value > afterIndex) {
      candidate = value;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return candidate;
}

function summarizeConsumptionLabels(
  labels: ConsumptionEventLabel[],
): Record<ConsumptionHorizon, ConsumptionLabelSummary> {
  const summarize = (horizon: ConsumptionHorizon): ConsumptionLabelSummary => {
    const consumedValues = labels
      .map((label) =>
        horizon === 12 ? label.consumedWithin12h : horizon === 24 ? label.consumedWithin24h : label.consumedWithin72h,
      )
      .filter((value): value is boolean => value != null);
    const censoredCount = labels.filter((label) =>
      horizon === 12 ? label.censoredWithin12h : horizon === 24 ? label.censoredWithin24h : label.censoredWithin72h,
    ).length;
    const usableCount = consumedValues.length;
    const consumedCount = consumedValues.filter(Boolean).length;
    return {
      usableCount,
      censoredCount,
      consumedCount,
      realizedRate: usableCount > 0 ? safeDiv(consumedCount, usableCount) : null,
    };
  };

  return {
    12: summarize(12),
    24: summarize(24),
    72: summarize(72),
  };
}

function priceStats(offers: Offer[]) {
  const prices = offers
    .map((offer) => offer.pricePerHour)
    .filter((value): value is number => value != null)
    .sort((a, b) => a - b);

  const medianPrice = percentile(prices, 0.5);
  const p10Price = percentile(prices, 0.1);
  const p90Price = percentile(prices, 0.9);
  const minPrice = prices.length > 0 ? prices[0] : null;
  const maxPrice = prices.length > 0 ? prices[prices.length - 1] : null;

  let priceCv: number | null = null;
  if (prices.length > 1 && medianPrice != null && medianPrice > 0) {
    const avg = mean(prices);
    const variance = mean(prices.map((value) => (value - avg) ** 2));
    priceCv = Math.sqrt(variance) / medianPrice;
  }

  return { prices, medianPrice, p10Price, p90Price, minPrice, maxPrice, priceCv };
}

function countTopShares(ids: Array<number | null>): { top1: number; top3: number } {
  const counts = new Map<number, number>();
  for (const id of ids) {
    if (id == null) continue;
    counts.set(id, (counts.get(id) ?? 0) + 1);
  }

  const sorted = [...counts.values()].sort((a, b) => b - a);
  const total = ids.length;
  return {
    top1: safeDiv(sorted[0] ?? 0, total),
    top3: safeDiv((sorted[0] ?? 0) + (sorted[1] ?? 0) + (sorted[2] ?? 0), total),
  };
}

function toTrendPoint(bucket: CohortBucket): TrendPoint {
  return {
    bucketStartUtc: bucket.bucketStartUtc,
    totalOffers: bucket.totalOffers,
    uniqueMachines: bucket.uniqueMachines,
    uniqueHosts: bucket.uniqueHosts,
    rentableOffers: bucket.rentableOffers,
    rentedOffers: bucket.rentedOffers,
    continuingOffers: bucket.continuingOffers,
    newOffers: bucket.newOffers,
    disappearedOffers: bucket.disappearedOffers,
    reappearedOffers: bucket.reappearedOffers,
    persistentDisappearedOffers: bucket.persistentDisappearedOffers,
    newOfferRate: bucket.newOfferRate,
    disappearedRate: bucket.disappearedRate,
    reappearedRate: bucket.reappearedRate,
    temporaryMissingRate: bucket.temporaryMissingRate,
    reappearedShortGapRate: bucket.reappearedShortGapRate,
    reappearedLongGapRate: bucket.reappearedLongGapRate,
    medianReappearanceDelayBuckets: bucket.medianReappearanceDelayBuckets,
    persistentDisappearanceRate: bucket.persistentDisappearanceRate,
    persistentDisappearanceRateN: bucket.persistentDisappearanceRateN,
    churnAdjustedDisappearanceRate: bucket.churnAdjustedDisappearanceRate,
    medianPrice: bucket.medianPrice,
    minPrice: bucket.minPrice,
    p10Price: bucket.p10Price,
    p90Price: bucket.p90Price,
    maxPrice: bucket.maxPrice,
    priceCv: bucket.priceCv,
    medianPriceChange: bucket.medianPriceChange,
    lowBandDisappearedRate: bucket.lowBandDisappearedRate,
    midBandDisappearedRate: bucket.midBandDisappearedRate,
    highBandDisappearedRate: bucket.highBandDisappearedRate,
    lowBandPersistentDisappearedRate: bucket.lowBandPersistentDisappearedRate,
    midBandPersistentDisappearedRate: bucket.midBandPersistentDisappearedRate,
    highBandPersistentDisappearedRate: bucket.highBandPersistentDisappearedRate,
    machineConcentrationShareTop1: bucket.machineConcentrationShareTop1,
    machineConcentrationShareTop3: bucket.machineConcentrationShareTop3,
    hostConcentrationShareTop1: bucket.hostConcentrationShareTop1,
    hostConcentrationShareTop3: bucket.hostConcentrationShareTop3,
    machinePersistenceRate: bucket.machinePersistenceRate,
    hostPersistenceRate: bucket.hostPersistenceRate,
    newMachineEntryRate: bucket.newMachineEntryRate,
    disappearingMachineRate: bucket.disappearingMachineRate,
    supplyTightnessScore: bucket.supplyTightnessScore,
    movementScore: bucket.movementScore,
    machineDepthScore: bucket.machineDepthScore,
    concentrationScore: bucket.concentrationScore,
    cohortPressureScore: bucket.cohortPressureScore,
    pressureAcceleration: bucket.pressureAcceleration,
    pressurePersistence: bucket.pressurePersistence,
    state: bucket.state,
    stateConfidence: bucket.stateConfidence,
    timeDepthScore: bucket.timeDepthScore,
    crossSectionDepthScore: bucket.crossSectionDepthScore,
    dataDepthScore: bucket.dataDepthScore,
    noiseScore: bucket.noiseScore,
    churnScore: bucket.churnScore,
    signalStrengthScore: bucket.signalStrengthScore,
    inferabilityScore: bucket.inferabilityScore,
    identityQualityScore: bucket.identityQualityScore,
    configVsFamilyPressureDelta: bucket.configVsFamilyPressureDelta,
    configVsFamilyPriceDelta: bucket.configVsFamilyPriceDelta,
    configVsFamilyHazardDelta: bucket.configVsFamilyHazardDelta,
  };
}

async function upsertRollupsFromLatestSnapshot() {
  const latest = await prisma.marketSnapshot.findFirst({
    orderBy: { capturedAt: "desc" },
    include: { offers: true },
  });

  if (!latest) return null;

  const groups = new Map<string, Offer[]>();
  for (const offer of latest.offers) {
    const key = `${offer.gpuName}::${offer.numGpus}::${normalizeOfferType(offer.offerType)}`;
    const current = groups.get(key) ?? [];
    current.push(offer);
    groups.set(key, current);
  }

  for (const [key, offers] of groups.entries()) {
    const [gpuName, numGpusRaw, offerType] = key.split("::");
    const numGpus = Number(numGpusRaw);
    const rentableOffers = offers.filter((offer) => offer.rentable).length;
    const rentedOffers = offers.filter((offer) => offer.rented).length;
    const impliedUtilization = safeDiv(rentedOffers, Math.max(offers.length, 1));
    const stats = priceStats(offers);

    await prisma.gpuRollup.upsert({
      where: {
        snapshotId_gpuName_numGpus_offerType: {
          snapshotId: latest.id,
          gpuName,
          numGpus,
          offerType,
        },
      },
      create: {
        snapshotId: latest.id,
        gpuName,
        numGpus,
        offerType,
        totalOffers: offers.length,
        rentableOffers,
        rentedOffers,
        impliedUtilization,
        minPrice: stats.minPrice,
        p10Price: stats.p10Price,
        medianPrice: stats.medianPrice,
        p90Price: stats.p90Price,
      },
      update: {
        totalOffers: offers.length,
        rentableOffers,
        rentedOffers,
        impliedUtilization,
        minPrice: stats.minPrice,
        p10Price: stats.p10Price,
        medianPrice: stats.medianPrice,
        p90Price: stats.p90Price,
      },
    });
  }

  return { snapshotId: latest.id, groupCount: groups.size };
}

async function main() {
  const rollupSummary = await upsertRollupsFromLatestSnapshot();

  const snapshotProfiles = await prisma.marketSnapshot.findMany({
    orderBy: { capturedAt: "asc" },
    select: { source: true, sourceQueryHash: true },
  });

  if (snapshotProfiles.length === 0) {
    console.log("No snapshots available for recompute.");
    return;
  }

  const latestQueryHashBySource = new Map<string, string>();
  for (const snapshot of snapshotProfiles) {
    latestQueryHashBySource.set(snapshot.source, snapshot.sourceQueryHash ?? "no-query-hash");
  }
  const latestProfileFilters = [...latestQueryHashBySource.entries()].map(([source, sourceQueryHash]) => ({
    source,
    sourceQueryHash: sourceQueryHash === "no-query-hash" ? null : sourceQueryHash,
  }));
  const snapshots = await prisma.marketSnapshot.findMany({
    where: { OR: latestProfileFilters },
    orderBy: { capturedAt: "asc" },
    include: { offers: true },
  });

  const lifecycleMap = new Map<string, LifecycleWorking>();
  const bucketsByKey = new Map<string, CohortBucket>();
  const historyByCohort = new Map<string, TrendPoint[]>();
  const sourceBucketHoursBySource = new Map<string, number>();
  const sourceFinalSnapshotBySource = new Map<string, Date>();

  const snapshotsBySource = new Map<string, typeof snapshots>();
  for (const snapshot of snapshots) {
    const latestQueryHash = latestQueryHashBySource.get(snapshot.source);
    if ((snapshot.sourceQueryHash ?? "no-query-hash") !== latestQueryHash) continue;
    const current = snapshotsBySource.get(snapshot.source) ?? [];
    current.push(snapshot);
    snapshotsBySource.set(snapshot.source, current);
  }

  for (const [source, sourceSnapshots] of snapshotsBySource.entries()) {
    const sortedSnapshots = [...sourceSnapshots].sort(
      (a, b) => a.capturedAt.getTime() - b.capturedAt.getTime(),
    );
    const sourceBucketHours = estimateSourceBucketHours(sortedSnapshots);
    const finalSnapshotAt = sortedSnapshots[sortedSnapshots.length - 1]?.capturedAt ?? new Date(0);
    sourceBucketHoursBySource.set(source, sourceBucketHours);
    sourceFinalSnapshotBySource.set(source, finalSnapshotAt);
    const pollGapSummary = summarizePollGapsMinutes(sortedSnapshots);
    const identityByOfferId = new Map<string, OfferIdentityResolved>();
    const getIdentity = (offer: Offer) => {
      const existing = identityByOfferId.get(offer.id);
      if (existing) return existing;
      const identity = inferOfferIdentity(offer, source);
      identityByOfferId.set(offer.id, identity);
      return identity;
    };
    const appearanceIndicesByFingerprint = new Map<string, number[]>();
    for (let i = 0; i < sortedSnapshots.length; i += 1) {
      for (const offer of sortedSnapshots[i].offers) {
        const stableFp = getIdentity(offer).stableOfferFingerprint;
        const current = appearanceIndicesByFingerprint.get(stableFp) ?? [];
        current.push(i);
        appearanceIndicesByFingerprint.set(stableFp, current);
      }
    }

    const previouslySeenFingerprints = new Set<string>();

    for (let i = 0; i < sortedSnapshots.length; i += 1) {
      const snapshot = sortedSnapshots[i];
      const previousSnapshot = i > 0 ? sortedSnapshots[i - 1] : null;

      const prevOffers = previousSnapshot?.offers ?? [];
      const currentOffers = snapshot.offers;

      const cohortPrevMap = new Map<string, Offer[]>();
      const cohortCurrentMap = new Map<string, Offer[]>();

      function push(map: Map<string, Offer[]>, key: string, offer: Offer) {
        const current = map.get(key) ?? [];
        current.push(offer);
        map.set(key, current);
      }

      for (const offer of prevOffers) {
        const exact = `${offer.gpuName}::${offer.numGpus}::${normalizeOfferType(offer.offerType)}`;
        const family = `${offer.gpuName}::combined::combined`;
        push(cohortPrevMap, exact, offer);
        push(cohortPrevMap, family, offer);
      }

      for (const offer of currentOffers) {
        const exact = `${offer.gpuName}::${offer.numGpus}::${normalizeOfferType(offer.offerType)}`;
        const family = `${offer.gpuName}::combined::combined`;
        push(cohortCurrentMap, exact, offer);
        push(cohortCurrentMap, family, offer);
      }

      const allCohorts = new Set<string>([...cohortPrevMap.keys(), ...cohortCurrentMap.keys()]);
      const futureIdentityByCohort = new Map<string, Array<Set<string>>>();
      for (let lookahead = 1; lookahead <= PERSISTENCE_BUCKETS; lookahead += 1) {
        const futureSnapshot = sortedSnapshots[i + lookahead];
        if (!futureSnapshot) break;
        for (const futureOffer of futureSnapshot.offers) {
          const exact = `${futureOffer.gpuName}::${futureOffer.numGpus}::${normalizeOfferType(futureOffer.offerType)}`;
          const family = `${futureOffer.gpuName}::combined::combined`;
          const stableFp = getIdentity(futureOffer).stableOfferFingerprint;

          const exactBuckets = futureIdentityByCohort.get(exact) ?? [];
          if (!exactBuckets[lookahead - 1]) exactBuckets[lookahead - 1] = new Set<string>();
          exactBuckets[lookahead - 1].add(stableFp);
          futureIdentityByCohort.set(exact, exactBuckets);

          const familyBuckets = futureIdentityByCohort.get(family) ?? [];
          if (!familyBuckets[lookahead - 1]) familyBuckets[lookahead - 1] = new Set<string>();
          familyBuckets[lookahead - 1].add(stableFp);
          futureIdentityByCohort.set(family, familyBuckets);
        }
      }

      for (const offer of currentOffers) {
        const identity = getIdentity(offer);
        const fp = identity.stableOfferFingerprint;
        const lifecycleKey = `${source}::${fp}`;

        const existing = lifecycleMap.get(lifecycleKey);
        if (!existing) {
          lifecycleMap.set(lifecycleKey, {
            source,
            offerFingerprint: fp,
            stableOfferFingerprint: fp,
            latestVersionFingerprint: identity.versionFingerprint,
            identityStrategy: identity.strategy,
            identityQualityScore: identity.identityQualityScore,
            offerExternalId: identity.offerExternalId ?? offer.offerId,
            gpuName: offer.gpuName,
            numGpus: offer.numGpus,
            offerType: normalizeOfferType(offer.offerType),
            machineId: offer.machineId,
            hostId: offer.hostId,
            firstSeenAt: snapshot.capturedAt,
            lastSeenAt: snapshot.capturedAt,
            totalVisibleSnapshots: 1,
            seenCount: 1,
            totalVisibleHours: 0,
            cumulativeVisibleMinutes: 0,
            offerSeenSpanMinutes: 0,
            disappearanceCount: 0,
            reappearanceCount: 0,
            firstMissingAt: null,
            reappearedAt: null,
            gapDurationMinutes: null,
            visibilitySegmentCount: 0,
            longestContinuousVisibleHours: 0,
            estimatedConsumedAt: null,
            insufficientObservation: false,
            latestKnownPricePerHour: offer.pricePerHour,
            latestKnownReliabilityScore: offer.reliabilityScore,
            firstKnownPricePerHour: offer.pricePerHour,
            minObservedPricePerHour: offer.pricePerHour,
            maxObservedPricePerHour: offer.pricePerHour,
            priceEditCount: 0,
            mutationCount: 0,
            lastStatus: "active",
            lastSeenSnapshotIndex: i,
            currentSegmentStart: snapshot.capturedAt,
            currentSegmentPrices: offer.pricePerHour == null ? [] : [offer.pricePerHour],
            currentSegmentStartPrice: offer.pricePerHour,
            currentSegmentStartRentable: offer.rentable,
            priorPrice: offer.pricePerHour,
            reappearanceGapBuckets: [],
            segments: [],
          });
        } else {
          const gap = i - existing.lastSeenSnapshotIndex;
          const hoursSinceLastSeen =
            (snapshot.capturedAt.getTime() - existing.lastSeenAt.getTime()) / 1000 / 3600;
          if (gap > 1) {
            existing.disappearanceCount += 1;
            existing.reappearanceCount += 1;
            existing.reappearanceGapBuckets.push(gap - 1);
            existing.lastStatus = "reappeared";
            existing.firstMissingAt =
              existing.firstMissingAt ??
              new Date(existing.lastSeenAt.getTime() + sourceBucketHours * 3600000);
            existing.reappearedAt = snapshot.capturedAt;
            existing.gapDurationMinutes = toMinutes(Math.max(0, hoursSinceLastSeen));
            existing.segments.push({
              segmentStartAt: existing.currentSegmentStart,
              segmentEndAt: existing.lastSeenAt,
              durationHours: (existing.lastSeenAt.getTime() - existing.currentSegmentStart.getTime()) / 1000 / 3600,
              endedBy: "disappeared",
              startPricePerHour: existing.currentSegmentStartPrice,
              endPricePerHour: existing.priorPrice,
              medianPricePerHour: percentile(existing.currentSegmentPrices, 0.5),
              startRentable: existing.currentSegmentStartRentable,
              endRentable: null,
            });
            existing.visibilitySegmentCount = existing.segments.length;
            existing.currentSegmentStart = snapshot.capturedAt;
            existing.currentSegmentPrices = [];
            existing.currentSegmentStartPrice = offer.pricePerHour;
            existing.currentSegmentStartRentable = offer.rentable;
          }

          existing.totalVisibleSnapshots += 1;
          existing.seenCount += 1;
          existing.totalVisibleHours += Math.max(0, Math.min(hoursSinceLastSeen, 1.5));
          existing.cumulativeVisibleMinutes = toMinutes(existing.totalVisibleHours);
          existing.lastSeenAt = snapshot.capturedAt;
          existing.offerSeenSpanMinutes = Math.max(
            0,
            (existing.lastSeenAt.getTime() - existing.firstSeenAt.getTime()) / 60000,
          );
          existing.lastSeenSnapshotIndex = i;
          const priorVersionFingerprint = existing.latestVersionFingerprint;
          existing.latestVersionFingerprint = identity.versionFingerprint;
          existing.identityStrategy = identity.strategy;
          existing.identityQualityScore =
            0.7 * existing.identityQualityScore + 0.3 * identity.identityQualityScore;
          existing.latestKnownPricePerHour = offer.pricePerHour;
          existing.latestKnownReliabilityScore = offer.reliabilityScore;
          existing.minObservedPricePerHour =
            existing.minObservedPricePerHour == null
              ? offer.pricePerHour
              : offer.pricePerHour == null
                ? existing.minObservedPricePerHour
                : Math.min(existing.minObservedPricePerHour, offer.pricePerHour);
          existing.maxObservedPricePerHour =
            existing.maxObservedPricePerHour == null
              ? offer.pricePerHour
              : offer.pricePerHour == null
                ? existing.maxObservedPricePerHour
                : Math.max(existing.maxObservedPricePerHour, offer.pricePerHour);

          if (
            offer.pricePerHour != null &&
            existing.priorPrice != null &&
            Math.abs(offer.pricePerHour - existing.priorPrice) > 1e-6
          ) {
            existing.priceEditCount += 1;
          }
          if (priorVersionFingerprint !== identity.versionFingerprint) {
            existing.mutationCount += 1;
          }

          existing.priorPrice = offer.pricePerHour;
          if (offer.pricePerHour != null) existing.currentSegmentPrices.push(offer.pricePerHour);

          const continuousHours =
            (snapshot.capturedAt.getTime() - existing.currentSegmentStart.getTime()) / 1000 / 3600;
          existing.longestContinuousVisibleHours = Math.max(
            existing.longestContinuousVisibleHours,
            continuousHours,
          );
          existing.lastStatus = "active";
        }

        previouslySeenFingerprints.add(fp);
      }

      for (const cohort of allCohorts) {
        const prev = cohortPrevMap.get(cohort) ?? [];
        const current = cohortCurrentMap.get(cohort) ?? [];

        const [gpuName, numGpusRaw, offerTypeRaw] = cohort.split("::");
        const numGpus = numGpusRaw === "combined" ? null : Number(numGpusRaw);
        const offerType = offerTypeRaw === "combined" ? null : offerTypeRaw;

        const prevMap = new Map<string, Offer>();
        const currentMap = new Map<string, Offer>();

        for (const offer of prev) {
          prevMap.set(getIdentity(offer).stableOfferFingerprint, offer);
        }
        for (const offer of current) {
          currentMap.set(getIdentity(offer).stableOfferFingerprint, offer);
        }

        const prevSet = new Set(prevMap.keys());
        const currentSet = new Set(currentMap.keys());

        let continuingOffers = 0;
        let newOffers = 0;
        let disappearedOffers = 0;
        let reappearedOffers = 0;
        let persistentDisappearedOffers = 0;
        let temporaryMissingOffers = 0;
        let rightCensoredDisappearedOffers = 0;
        const reappearanceDelays: number[] = [];
        const consumptionEventLabels: Array<ConsumptionEventLabel & { stableOfferFingerprint: string }> = [];

        for (const id of currentSet) {
          if (prevSet.has(id)) continuingOffers += 1;
          else {
            newOffers += 1;
            if (previouslySeenFingerprints.has(id)) reappearedOffers += 1;
          }
        }

        const futureBuckets = futureIdentityByCohort.get(cohort) ?? [];
        for (const id of prevSet) {
          if (!currentSet.has(id)) {
            disappearedOffers += 1;
            const nextAppearanceIndex = findNextAppearanceIndex(
              appearanceIndicesByFingerprint.get(id) ?? [],
              i,
            );
            const timeToReappearanceBuckets =
              nextAppearanceIndex == null ? null : Math.max(1, nextAppearanceIndex - i);
            const futureHoursAvailable =
              (finalSnapshotAt.getTime() - snapshot.capturedAt.getTime()) / 3600000;
            const label = buildConsumptionEventLabel({
              timeToReappearanceBuckets,
              sourceBucketHours,
              futureHoursAvailable,
            });
            consumptionEventLabels.push({
              stableOfferFingerprint: id,
              ...label,
            });

            const disappearanceOutcome = classifyDisappearanceOutcome({
              id,
              futureBuckets,
              shortGapMaxBuckets: SHORT_GAP_MAX_BUCKETS,
            });
            if (disappearanceOutcome === "persistently_disappeared") {
              if (futureBuckets.length < PERSISTENCE_BUCKETS) {
                // Not enough lookahead buckets to classify durable exit; treat as right-censored.
                rightCensoredDisappearedOffers += 1;
              } else {
                persistentDisappearedOffers += 1;
              }
            } else {
              temporaryMissingOffers += 1;
              for (let bucketOffset = 0; bucketOffset < futureBuckets.length; bucketOffset += 1) {
                if (futureBuckets[bucketOffset]?.has(id)) {
                  reappearanceDelays.push(bucketOffset + 1);
                  break;
                }
              }
            }
          }
        }
        const reappearanceSummary = summarizeReappearanceGaps(reappearanceDelays, SHORT_GAP_MAX_BUCKETS);
        const consumptionLabelSummaryByHorizon = summarizeConsumptionLabels(consumptionEventLabels);
        const labelabilityScore =
          100 *
          clamp(
            0.5 * clamp(safeDiv(consumptionLabelSummaryByHorizon[24].usableCount, Math.max(disappearedOffers, 1))) +
              0.2 * (1 - clamp(safeDiv(consumptionLabelSummaryByHorizon[24].censoredCount, Math.max(disappearedOffers, 1)))) +
              0.3 * clamp(safeDiv(futureBuckets.length * sourceBucketHours, 24)),
          );
        const evaluableDisappearedOffers = Math.max(
          0,
          disappearedOffers - rightCensoredDisappearedOffers,
        );

        const priorCount = Math.max(prevSet.size, 1);

        const stats = priceStats(current);
        const prevStats = priceStats(prev);
        const futureWindowCoverage12h = clamp(safeDiv(futureBuckets.length * sourceBucketHours, 12));
        const futureWindowCoverage24h = clamp(safeDiv(futureBuckets.length * sourceBucketHours, 24));
        const futureWindowCoverage72h = clamp(safeDiv(futureBuckets.length * sourceBucketHours, 72));

        const rentableOffers = current.filter((offer) => offer.rentable).length;
        const rentedOffers = current.filter((offer) => offer.rented).length;
        const impliedUtilization = safeDiv(current.length - rentableOffers, Math.max(current.length, 1));
        const availabilityRatio = safeDiv(rentableOffers, Math.max(current.length, 1));

        const machineIds = current.map((offer) => offer.machineId);
        const hostIds = current.map((offer) => offer.hostId);
        const prevMachineSet = new Set(prev.map((offer) => offer.machineId).filter((id): id is number => id != null));
        const currMachineSet = new Set(current.map((offer) => offer.machineId).filter((id): id is number => id != null));
        const prevHostSet = new Set(prev.map((offer) => offer.hostId).filter((id): id is number => id != null));
        const currHostSet = new Set(current.map((offer) => offer.hostId).filter((id): id is number => id != null));

        let machineContinuing = 0;
        for (const id of currMachineSet) if (prevMachineSet.has(id)) machineContinuing += 1;
        let hostContinuing = 0;
        for (const id of currHostSet) if (prevHostSet.has(id)) hostContinuing += 1;

        let machineEntries = 0;
        for (const id of currMachineSet) if (!prevMachineSet.has(id)) machineEntries += 1;
        let machineExits = 0;
        for (const id of prevMachineSet) if (!currMachineSet.has(id)) machineExits += 1;

        let hostEntries = 0;
        for (const id of currHostSet) if (!prevHostSet.has(id)) hostEntries += 1;
        let hostExits = 0;
        for (const id of prevHostSet) if (!currHostSet.has(id)) hostExits += 1;

        const machineShare = countTopShares(machineIds);
        const hostShare = countTopShares(hostIds);
        const identityQualityScore =
          (mean(current.map((offer) => getIdentity(offer).identityQualityScore)) || 0) * 100;

        const p10 = prevStats.p10Price;
        const p90 = prevStats.p90Price;

        let lowBandPrior = 0;
        let midBandPrior = 0;
        let highBandPrior = 0;
        let lowBandDisappeared = 0;
        let midBandDisappeared = 0;
        let highBandDisappeared = 0;

        if (p10 != null && p90 != null) {
          for (const offer of prev) {
            if (offer.pricePerHour == null) continue;
            if (offer.pricePerHour <= p10) lowBandPrior += 1;
            else if (offer.pricePerHour >= p90) highBandPrior += 1;
            else midBandPrior += 1;
          }
          for (const [id, offer] of prevMap.entries()) {
            if (currentSet.has(id) || offer.pricePerHour == null) continue;
            if (offer.pricePerHour <= p10) lowBandDisappeared += 1;
            else if (offer.pricePerHour >= p90) highBandDisappeared += 1;
            else midBandDisappeared += 1;
          }
        }

        const priorPressureSeries = historyByCohort.get(
          makeCohortKey({ source, gpuName, numGpus, offerType }),
        ) ?? [];
        const observationCount = priorPressureSeries.length + 1;
        const offerSeenSpanMinutes = Math.max(0, observationCount - 1) * toMinutes(sourceBucketHours);
        const expectedObservations = Math.max(
          1,
          Math.floor(offerSeenSpanMinutes / Math.max(1, pollGapSummary.medianPollGapMinutes)) + 1,
        );
        const coverageRatio = clamp(safeDiv(observationCount, expectedObservations));
        const observationsPerOffer = safeDiv(observationCount, Math.max(current.length, 1));
        const samplingQualityScore = computeSamplingQualityScore({
          observationCount,
          observationsPerOffer,
          medianPollGapMinutes: pollGapSummary.medianPollGapMinutes,
          maxPollGapMinutes: pollGapSummary.maxPollGapMinutes,
          coverageRatio,
          futureWindowCoverage24h,
        });
        const priorPressure = priorPressureSeries.length > 0 ? priorPressureSeries[priorPressureSeries.length - 1].cohortPressureScore ?? 50 : 50;
        const priorPressure2 = priorPressureSeries.length > 1 ? priorPressureSeries[priorPressureSeries.length - 2].cohortPressureScore ?? priorPressure : priorPressure;

        const pressure = computeCohortPressureScore({
          persistentDisappearanceRate: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          disappearedRate: safeDiv(disappearedOffers, priorCount),
          newOfferRate: safeDiv(newOffers, priorCount),
          lowBandPersistentDisappearedRate: safeDiv(lowBandDisappeared, Math.max(lowBandPrior, 1)),
          medianPriceChangePct:
            stats.medianPrice == null || prevStats.medianPrice == null || prevStats.medianPrice <= 0
              ? 0
              : (stats.medianPrice - prevStats.medianPrice) / prevStats.medianPrice,
          rentableShare: availabilityRatio,
          uniqueMachineCount: currMachineSet.size,
          machineConcentrationShareTop3: machineShare.top3,
          reappearedRate: safeDiv(reappearedOffers, Math.max(newOffers, 1)),
          temporaryMissingRate: safeDiv(temporaryMissingOffers, Math.max(disappearedOffers, 1)),
          identityQualityScore,
          priorPressure,
          priorPressure2,
        });

        const timeDepthScore = 100 * Math.min(1, safeDiv(priorPressureSeries.length + 1, 48));
        const crossSectionDepthScore =
          100 *
          Math.min(
            1,
            0.55 * safeDiv(currMachineSet.size, 20) +
              0.3 * safeDiv(currHostSet.size, 16) +
              0.15 * safeDiv(current.length, 50),
          );
        // v3.2: require both temporal continuity and cross-sectional competition depth.
        const dataDepthScore = combineDepthScores(timeDepthScore, crossSectionDepthScore);
        const lifecycleObservabilityScore = computeLifecycleObservabilityScore({
          labelabilityScore,
          offerSeenSpanMinutes,
          futureWindowCoverage72h,
          historyContinuity: timeDepthScore,
        });
        const cohortObservationDensityScore =
          100 *
          clamp(
            0.4 * clamp(samplingQualityScore / 100) +
              0.3 * clamp(coverageRatio) +
              0.3 * clamp(lifecycleObservabilityScore / 100),
          );
        const insufficientSampling =
          samplingQualityScore < 45 ||
          observationCount < 8 ||
          pollGapSummary.maxPollGapMinutes > 20;

        const candidatePoint: TrendPoint = {
          bucketStartUtc: floorToUtcHalfHour(snapshot.capturedAt),
          totalOffers: current.length,
          uniqueMachines: currMachineSet.size,
          uniqueHosts: currHostSet.size,
          rentableOffers,
          disappearedRate: safeDiv(disappearedOffers, priorCount),
          reappearedRate: safeDiv(reappearedOffers, Math.max(newOffers, 1)),
          temporaryMissingRate: safeDiv(temporaryMissingOffers, Math.max(disappearedOffers, 1)),
          reappearedShortGapRate: reappearanceSummary.shortGapReappearanceRate,
          reappearedLongGapRate: reappearanceSummary.longGapReappearanceRate,
          medianReappearanceDelayBuckets: reappearanceSummary.medianDelayBuckets,
          persistentDisappearanceRate: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          persistentDisappearanceRateN: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          churnAdjustedDisappearanceRate: pressure.churnAdjustedDisappearanceRate,
          medianPrice: stats.medianPrice,
          cohortPressureScore: pressure.cohortPressureScore,
          movementScore: pressure.movementScore,
          churnScore: pressure.churnScore,
          signalStrengthScore: pressure.signalStrengthScore,
          inferabilityScore: pressure.inferabilityScore,
          identityQualityScore,
          timeDepthScore,
          crossSectionDepthScore,
        };
        const noiseScore = computeNoiseScore([...priorPressureSeries.slice(-20), candidatePoint]);
        const churnPenaltyScore =
          100 *
          Math.min(
            1,
            0.6 * safeDiv(reappearedOffers, Math.max(disappearedOffers, 1)) +
              0.4 * safeDiv(temporaryMissingOffers, Math.max(disappearedOffers, 1)),
          );
        const adjustedSignalStrengthScore = Math.max(
          0,
          Math.min(
            100,
            pressure.signalStrengthScore -
              0.4 * noiseScore -
              0.35 * churnPenaltyScore +
              0.18 * dataDepthScore +
              0.12 * identityQualityScore,
          ),
        );
        const adjustedInferabilityScore = Math.max(
          0,
          Math.min(
            100,
            0.5 * adjustedSignalStrengthScore +
              0.25 * dataDepthScore +
              0.15 * identityQualityScore -
              0.4 * noiseScore -
              0.35 * churnPenaltyScore,
          ),
        );

        const state = classifyCohortState({
          dataDepthScore,
          noiseScore,
          cohortPressureScore: pressure.cohortPressureScore,
          pressureAcceleration: pressure.pressureAcceleration,
          persistentDisappearanceRate: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          reappearedRate: safeDiv(reappearedOffers, Math.max(newOffers, 1)),
          churnScore: pressure.churnScore,
          signalStrengthScore: adjustedSignalStrengthScore,
          inferabilityScore: adjustedInferabilityScore,
          identityQualityScore,
        });

        const stateConfidence = computeStateConfidence({
          dataDepthScore,
          historyDepth: priorPressureSeries.length + 1,
          machineCount: currMachineSet.size,
          noiseScore,
          reappearedRate: safeDiv(reappearedOffers, Math.max(newOffers, 1)),
          inferabilityScore: adjustedInferabilityScore,
          identityQualityScore,
        });

        const bucket: CohortBucket = {
          key: { source, gpuName, numGpus, offerType },
          bucketStartUtc: floorToUtcHalfHour(snapshot.capturedAt),
          snapshotId: snapshot.id,
          snapshotCount: 1,
          totalOffers: current.length,
          uniqueMachines: currMachineSet.size,
          uniqueHosts: currHostSet.size,
          rentableOffers,
          rentedOffers,
          continuingOffers,
          newOffers,
          disappearedOffers,
          reappearedOffers,
          persistentDisappearedOffers,
          newOfferRate: safeDiv(newOffers, priorCount),
          disappearedRate: safeDiv(disappearedOffers, priorCount),
          reappearedRate: safeDiv(reappearedOffers, Math.max(newOffers, 1)),
          temporaryMissingRate: safeDiv(temporaryMissingOffers, Math.max(disappearedOffers, 1)),
          reappearedShortGapRate: reappearanceSummary.shortGapReappearanceRate,
          reappearedLongGapRate: reappearanceSummary.longGapReappearanceRate,
          medianReappearanceDelayBuckets: reappearanceSummary.medianDelayBuckets,
          persistentDisappearanceRate: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          persistentDisappearanceRateN: safeDiv(
            persistentDisappearedOffers,
            Math.max(evaluableDisappearedOffers, 1),
          ),
          churnAdjustedDisappearanceRate: pressure.churnAdjustedDisappearanceRate,
          machineEntryRate: safeDiv(machineEntries, Math.max(prevMachineSet.size, 1)),
          machineExitRate: safeDiv(machineExits, Math.max(prevMachineSet.size, 1)),
          hostEntryRate: safeDiv(hostEntries, Math.max(prevHostSet.size, 1)),
          hostExitRate: safeDiv(hostExits, Math.max(prevHostSet.size, 1)),
          impliedUtilization,
          availabilityRatio,
          minPrice: stats.minPrice,
          p10Price: stats.p10Price,
          medianPrice: stats.medianPrice,
          p90Price: stats.p90Price,
          maxPrice: stats.maxPrice,
          priceCv: stats.priceCv,
          medianPriceChange:
            stats.medianPrice == null || prevStats.medianPrice == null
              ? null
              : stats.medianPrice - prevStats.medianPrice,
          lowBandDisappearedRate: safeDiv(lowBandDisappeared, Math.max(lowBandPrior, 1)),
          midBandDisappearedRate: safeDiv(midBandDisappeared, Math.max(midBandPrior, 1)),
          highBandDisappearedRate: safeDiv(highBandDisappeared, Math.max(highBandPrior, 1)),
          lowBandPersistentDisappearedRate: safeDiv(lowBandDisappeared, Math.max(lowBandPrior, 1)),
          midBandPersistentDisappearedRate: safeDiv(midBandDisappeared, Math.max(midBandPrior, 1)),
          highBandPersistentDisappearedRate: safeDiv(highBandDisappeared, Math.max(highBandPrior, 1)),
          supplyTightnessScore: pressure.supplyTightnessScore,
          movementScore: pressure.movementScore,
          machineDepthScore: pressure.machineDepthScore,
          concentrationScore: pressure.concentrationScore,
          cohortPressureScore: pressure.cohortPressureScore,
          pressureAcceleration: pressure.pressureAcceleration,
          pressurePersistence: pressure.pressurePersistence,
          state,
          stateConfidence,
          timeDepthScore,
          crossSectionDepthScore,
          dataDepthScore,
          noiseScore,
          churnScore: pressure.churnScore,
          signalStrengthScore: adjustedSignalStrengthScore,
          inferabilityScore: adjustedInferabilityScore,
          identityQualityScore,
          observationCount,
          observationsPerOffer,
          medianPollGapMinutes: pollGapSummary.medianPollGapMinutes,
          maxPollGapMinutes: pollGapSummary.maxPollGapMinutes,
          coverageRatio,
          offerSeenSpanMinutes,
          cohortObservationDensityScore,
          labelabilityScore,
          futureWindowCoverage12h,
          futureWindowCoverage24h,
          futureWindowCoverage72h,
          samplingQualityScore,
          lifecycleObservabilityScore,
          insufficientSampling,
          configVsFamilyPressureDelta: null,
          configVsFamilyPriceDelta: null,
          configVsFamilyHazardDelta: null,
          machineConcentrationShareTop1: machineShare.top1,
          machineConcentrationShareTop3: machineShare.top3,
          hostConcentrationShareTop1: hostShare.top1,
          hostConcentrationShareTop3: hostShare.top3,
          machinePersistenceRate: safeDiv(machineContinuing, Math.max(prevMachineSet.size, 1)),
          hostPersistenceRate: safeDiv(hostContinuing, Math.max(prevHostSet.size, 1)),
          newMachineEntryRate: safeDiv(machineEntries, Math.max(currMachineSet.size, 1)),
          disappearingMachineRate: safeDiv(machineExits, Math.max(prevMachineSet.size, 1)),
          consumptionEventLabels,
          consumptionLabelSummaryByHorizon,
        };

        const key = `${makeCohortKey(bucket.key)}::${bucket.bucketStartUtc.toISOString()}`;
        bucketsByKey.set(key, bucket);

        const history = historyByCohort.get(makeCohortKey(bucket.key)) ?? [];
        history.push(toTrendPoint(bucket));
        historyByCohort.set(makeCohortKey(bucket.key), history);
      }
    }

    for (const lifecycle of lifecycleMap.values()) {
      if (lifecycle.source !== source) continue;
      lifecycle.segments.push({
        segmentStartAt: lifecycle.currentSegmentStart,
        segmentEndAt: lifecycle.lastSeenAt,
        durationHours:
          (lifecycle.lastSeenAt.getTime() - lifecycle.currentSegmentStart.getTime()) / 1000 / 3600,
        endedBy: "still_active",
        startPricePerHour: lifecycle.currentSegmentStartPrice,
        endPricePerHour: lifecycle.priorPrice,
        medianPricePerHour: percentile(lifecycle.currentSegmentPrices, 0.5),
        startRentable: lifecycle.currentSegmentStartRentable,
        endRentable: null,
      });
      lifecycle.visibilitySegmentCount = lifecycle.segments.length;
      lifecycle.offerSeenSpanMinutes = Math.max(
        0,
        (lifecycle.lastSeenAt.getTime() - lifecycle.firstSeenAt.getTime()) / 60000,
      );
      const bucketHours = sourceBucketHoursBySource.get(lifecycle.source) ?? 0.5;
      const sourceFinalSnapshotAt = sourceFinalSnapshotBySource.get(lifecycle.source) ?? lifecycle.lastSeenAt;
      const staleMinutes = Math.max(
        0,
        (sourceFinalSnapshotAt.getTime() - lifecycle.lastSeenAt.getTime()) / 60000,
      );
      if (staleMinutes >= Math.max(30, toMinutes(bucketHours) * 2)) {
        lifecycle.estimatedConsumedAt = new Date(
          lifecycle.lastSeenAt.getTime() + Math.round(bucketHours * 3600000),
        );
      }
      lifecycle.insufficientObservation =
        lifecycle.totalVisibleSnapshots < 3 ||
        lifecycle.offerSeenSpanMinutes < Math.max(45, toMinutes(bucketHours) * 2);
    }
  }

  const bucketRows = [...bucketsByKey.values()].sort(
    (a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime(),
  );

  const familyLookup = new Map<string, CohortBucket>();
  for (const bucket of bucketRows) {
    if (bucket.key.numGpus == null && bucket.key.offerType == null) {
      const key = `${bucket.key.source}::${bucket.key.gpuName}::${bucket.bucketStartUtc.toISOString()}`;
      familyLookup.set(key, bucket);
    }
  }

  for (const bucket of bucketRows) {
    if (bucket.key.numGpus == null && bucket.key.offerType == null) continue;
    const familyKey = `${bucket.key.source}::${bucket.key.gpuName}::${bucket.bucketStartUtc.toISOString()}`;
    const family = familyLookup.get(familyKey);
    if (!family) continue;

    bucket.configVsFamilyPressureDelta = bucket.cohortPressureScore - family.cohortPressureScore;
    bucket.configVsFamilyPriceDelta =
      bucket.medianPrice == null || family.medianPrice == null ? null : bucket.medianPrice - family.medianPrice;
    bucket.configVsFamilyHazardDelta =
      bucket.persistentDisappearanceRate - family.persistentDisappearanceRate;

    bucket.cohortPressureScore = shrinkTowardsFamily(
      bucket.cohortPressureScore,
      family.cohortPressureScore,
      bucket.totalOffers,
    );
  }

  await prisma.gpuTrendAggregate.deleteMany({});
  for (const bucket of bucketRows) {
    await prisma.gpuTrendAggregate.create({
      data: {
        snapshotId: bucket.snapshotId,
        source: bucket.key.source,
        cohortKey: makeCohortKey(bucket.key),
        gpuName: bucket.key.gpuName,
        numGpus: bucket.key.numGpus,
        offerType: bucket.key.offerType,
        bucketStartUtc: bucket.bucketStartUtc,
        snapshotCount: bucket.snapshotCount,
        totalOffers: bucket.totalOffers,
        uniqueMachines: bucket.uniqueMachines,
        uniqueHosts: bucket.uniqueHosts,
        rentableOffers: bucket.rentableOffers,
        rentedOffers: bucket.rentedOffers,
        continuingOffers: bucket.continuingOffers,
        newOffers: bucket.newOffers,
        disappearedOffers: bucket.disappearedOffers,
        reappearedOffers: bucket.reappearedOffers,
        persistentDisappearedOffers: bucket.persistentDisappearedOffers,
        newOfferRate: bucket.newOfferRate,
        disappearedRate: bucket.disappearedRate,
        reappearedRate: bucket.reappearedRate,
        temporaryMissingRate: bucket.temporaryMissingRate,
        reappearedShortGapRate: bucket.reappearedShortGapRate,
        reappearedLongGapRate: bucket.reappearedLongGapRate,
        medianReappearanceDelayBuckets: bucket.medianReappearanceDelayBuckets,
        persistentDisappearanceRate: bucket.persistentDisappearanceRate,
        persistentDisappearanceRateN: bucket.persistentDisappearanceRateN,
        churnAdjustedDisappearanceRate: bucket.churnAdjustedDisappearanceRate,
        machineEntryRate: bucket.machineEntryRate,
        machineExitRate: bucket.machineExitRate,
        hostEntryRate: bucket.hostEntryRate,
        hostExitRate: bucket.hostExitRate,
        impliedUtilization: bucket.impliedUtilization,
        availabilityRatio: bucket.availabilityRatio,
        minPrice: bucket.minPrice,
        p10Price: bucket.p10Price,
        medianPrice: bucket.medianPrice,
        p90Price: bucket.p90Price,
        maxPrice: bucket.maxPrice,
        priceCv: bucket.priceCv,
        medianPriceChange: bucket.medianPriceChange,
        lowBandDisappearedRate: bucket.lowBandDisappearedRate,
        midBandDisappearedRate: bucket.midBandDisappearedRate,
        highBandDisappearedRate: bucket.highBandDisappearedRate,
        lowBandPersistentDisappearedRate: bucket.lowBandPersistentDisappearedRate,
        midBandPersistentDisappearedRate: bucket.midBandPersistentDisappearedRate,
        highBandPersistentDisappearedRate: bucket.highBandPersistentDisappearedRate,
        supplyTightnessScore: bucket.supplyTightnessScore,
        movementScore: bucket.movementScore,
        machineDepthScore: bucket.machineDepthScore,
        concentrationScore: bucket.concentrationScore,
        cohortPressureScore: bucket.cohortPressureScore,
        pressureAcceleration: bucket.pressureAcceleration,
        pressurePersistence: bucket.pressurePersistence,
        state: bucket.state,
        stateConfidence: bucket.stateConfidence,
        timeDepthScore: bucket.timeDepthScore,
        crossSectionDepthScore: bucket.crossSectionDepthScore,
        dataDepthScore: bucket.dataDepthScore,
        noiseScore: bucket.noiseScore,
        churnScore: bucket.churnScore,
        signalStrengthScore: bucket.signalStrengthScore,
        inferabilityScore: bucket.inferabilityScore,
        identityQualityScore: bucket.identityQualityScore,
        observationCount: bucket.observationCount,
        observationsPerOffer: bucket.observationsPerOffer,
        medianPollGapMinutes: bucket.medianPollGapMinutes,
        maxPollGapMinutes: bucket.maxPollGapMinutes,
        coverageRatio: bucket.coverageRatio,
        offerSeenSpanMinutes: bucket.offerSeenSpanMinutes,
        cohortObservationDensityScore: bucket.cohortObservationDensityScore,
        labelabilityScore: bucket.labelabilityScore,
        futureWindowCoverage12h: bucket.futureWindowCoverage12h,
        futureWindowCoverage24h: bucket.futureWindowCoverage24h,
        futureWindowCoverage72h: bucket.futureWindowCoverage72h,
        samplingQualityScore: bucket.samplingQualityScore,
        lifecycleObservabilityScore: bucket.lifecycleObservabilityScore,
        insufficientSampling: bucket.insufficientSampling,
        configVsFamilyPressureDelta: bucket.configVsFamilyPressureDelta,
        configVsFamilyPriceDelta: bucket.configVsFamilyPriceDelta,
        configVsFamilyHazardDelta: bucket.configVsFamilyHazardDelta,
        machineConcentrationShareTop1: bucket.machineConcentrationShareTop1,
        machineConcentrationShareTop3: bucket.machineConcentrationShareTop3,
        hostConcentrationShareTop1: bucket.hostConcentrationShareTop1,
        hostConcentrationShareTop3: bucket.hostConcentrationShareTop3,
        machinePersistenceRate: bucket.machinePersistenceRate,
        hostPersistenceRate: bucket.hostPersistenceRate,
        newMachineEntryRate: bucket.newMachineEntryRate,
        disappearingMachineRate: bucket.disappearingMachineRate,
      },
    });
  }

  await prisma.offerLifecycleSegment.deleteMany({});
  await prisma.offerLifecycle.deleteMany({});

  for (const lifecycle of lifecycleMap.values()) {
    const created = await prisma.offerLifecycle.create({
      data: {
        source: lifecycle.source,
        offerFingerprint: lifecycle.offerFingerprint,
        stableOfferFingerprint: lifecycle.stableOfferFingerprint,
        latestVersionFingerprint: lifecycle.latestVersionFingerprint,
        identityStrategy: lifecycle.identityStrategy,
        identityQualityScore: lifecycle.identityQualityScore,
        offerExternalId: lifecycle.offerExternalId,
        gpuName: lifecycle.gpuName,
        numGpus: lifecycle.numGpus,
        offerType: lifecycle.offerType,
        machineId: lifecycle.machineId,
        hostId: lifecycle.hostId,
        firstSeenAt: lifecycle.firstSeenAt,
        lastSeenAt: lifecycle.lastSeenAt,
        totalVisibleSnapshots: lifecycle.totalVisibleSnapshots,
        seenCount: lifecycle.seenCount,
        totalVisibleHours: lifecycle.totalVisibleHours,
        cumulativeVisibleMinutes: lifecycle.cumulativeVisibleMinutes,
        offerSeenSpanMinutes: lifecycle.offerSeenSpanMinutes,
        disappearanceCount: lifecycle.disappearanceCount,
        reappearanceCount: lifecycle.reappearanceCount,
        firstMissingAt: lifecycle.firstMissingAt,
        reappearedAt: lifecycle.reappearedAt,
        gapDurationMinutes: lifecycle.gapDurationMinutes,
        visibilitySegmentCount: lifecycle.visibilitySegmentCount,
        longestContinuousVisibleHours: lifecycle.longestContinuousVisibleHours,
        estimatedConsumedAt: lifecycle.estimatedConsumedAt,
        insufficientObservation: lifecycle.insufficientObservation,
        latestKnownPricePerHour: lifecycle.latestKnownPricePerHour,
        latestKnownReliabilityScore: lifecycle.latestKnownReliabilityScore,
        firstKnownPricePerHour: lifecycle.firstKnownPricePerHour,
        minObservedPricePerHour: lifecycle.minObservedPricePerHour,
        maxObservedPricePerHour: lifecycle.maxObservedPricePerHour,
        priceEditCount: lifecycle.priceEditCount,
        mutationCount: lifecycle.mutationCount,
        lastStatus: lifecycle.lastStatus,
      },
    });

    if (lifecycle.segments.length > 0) {
      await prisma.offerLifecycleSegment.createMany({
        data: lifecycle.segments.map((segment) => ({
          lifecycleId: created.id,
          segmentStartAt: segment.segmentStartAt,
          segmentEndAt: segment.segmentEndAt,
          durationHours: segment.durationHours,
          endedBy: segment.endedBy,
          startPricePerHour: segment.startPricePerHour,
          endPricePerHour: segment.endPricePerHour,
          medianPricePerHour: segment.medianPricePerHour,
          startRentable: segment.startRentable,
          endRentable: segment.endRentable,
        })),
      });
    }
  }

  await prisma.cohortForecast.deleteMany({});

  const coreForecastByKey = new Map<
    string,
    { pTight24h: number; pTight72h: number; pTight7d: number; pPriceUp24h: number }
  >();

  for (const bucket of bucketRows) {
    const forecast = forecastProbabilitiesFromState({
      state: bucket.state,
      pressure: bucket.cohortPressureScore,
      pressureAcceleration: bucket.pressureAcceleration,
      confidenceScore: bucket.stateConfidence,
      configVsFamilyDelta: bucket.configVsFamilyPressureDelta ?? 0,
      inferabilityScore: bucket.inferabilityScore,
      signalStrengthScore: bucket.signalStrengthScore,
    });

    const horizons = [24, 72, 168];
    for (const horizon of horizons) {
      const pTight = horizon === 24 ? forecast.pTight24h : horizon === 72 ? forecast.pTight72h : forecast.pTight7d;
      const pPriceUp = forecast.pPriceUp24h * (horizon === 24 ? 1 : horizon === 72 ? 0.9 : 0.82);
      const pPriceDown = forecast.pPriceDown24h * (horizon === 24 ? 1 : horizon === 72 ? 0.92 : 0.86);
      const pPriceFlat = Math.max(0, 1 - pPriceUp - pPriceDown);

      await prisma.cohortForecast.create({
        data: {
          source: bucket.key.source,
          gpuName: bucket.key.gpuName,
          numGpus: bucket.key.numGpus,
          offerType: bucket.key.offerType,
          bucketStartUtc: bucket.bucketStartUtc,
          forecastHorizonHours: horizon,
          pTight,
          pBalanced: Math.max(0, 1 - pTight - 0.12),
          pOversupplied: Math.max(0, 1 - pTight - Math.max(0, 1 - pTight - 0.12)),
          pPriceUp,
          pPriceFlat,
          pPriceDown,
          expectedPressure: bucket.cohortPressureScore,
          expectedPressureLow: Math.max(0, bucket.cohortPressureScore - (100 - bucket.stateConfidence) * 0.18),
          expectedPressureHigh: Math.min(100, bucket.cohortPressureScore + (100 - bucket.stateConfidence) * 0.18),
          expectedVisibleSupply: bucket.totalOffers,
          expectedVisibleSupplyLow: Math.max(0, bucket.totalOffers * (1 - bucket.persistentDisappearanceRate)),
          expectedVisibleSupplyHigh: bucket.totalOffers * (1 + bucket.newOfferRate),
          expectedMedianPrice: bucket.medianPrice,
          expectedMedianPriceLow: bucket.medianPrice == null ? null : bucket.medianPrice * (1 - pPriceDown * 0.12),
          expectedMedianPriceHigh: bucket.medianPrice == null ? null : bucket.medianPrice * (1 + pPriceUp * 0.12),
          confidenceScore: bucket.stateConfidence,
          calibrationVersion: CALIBRATION_VERSION,
          modelVersion: MODEL_VERSION,
        },
      });
    }

    coreForecastByKey.set(`${makeCohortKey(bucket.key)}::${bucket.bucketStartUtc.toISOString()}`, {
      pTight24h: forecast.pTight24h,
      pTight72h: forecast.pTight72h,
      pTight7d: forecast.pTight7d,
      pPriceUp24h: forecast.pPriceUp24h,
    });
  }

  await prisma.forecastBacktest.deleteMany({});

  const rowsByCohort = new Map<string, CohortBucket[]>();
  for (const row of bucketRows) {
    const key = makeCohortKey(row.key);
    const current = rowsByCohort.get(key) ?? [];
    current.push(row);
    rowsByCohort.set(key, current);
  }

  type PendingBacktestRow = {
    source: string;
    gpuName: string;
    numGpus: number | null;
    offerType: string | null;
    predictionBucketStartUtc: Date;
    horizonHours: number;
    predictedPTight: number;
    realizedTight: boolean;
    predictedPPriceUp: number;
    realizedPriceUp: boolean;
    predictedConsumptionProbRaw: number;
    predictedConsumptionProbCalibrated: number;
    predictedConsumptionProb: number;
    realizedConsumption: boolean;
    realizedConsumptionLegacy: boolean;
    consumptionLabelCensored: boolean;
    consumptionLabelQuality: "usable" | "censored" | "under-observed" | "ambiguous-reappearance";
    timeToReappearanceBuckets: number | null;
    timeToReappearanceHours: number | null;
    realizedConsumedWithin12h: boolean | null;
    realizedConsumedWithin24h: boolean | null;
    realizedConsumedWithin72h: boolean | null;
    realizedTightSustained: boolean;
    confidenceBucket: "low" | "medium" | "high";
    inferabilityBucket: "low" | "medium" | "high";
    stateAtPrediction: CohortState;
    calibrationBucket: string;
    modelVersion: string;
  };

  const pendingRows: PendingBacktestRow[] = [];
  const consumptionUsableCountsByHorizon = new Map<number, number>();
  const consumptionCensoredCountsByHorizon = new Map<number, number>();
  const consumptionCoverageByCohort = new Map<string, { usable12: number; censored12: number; usable24: number; censored24: number; usable72: number; censored72: number }>();

  for (const [cohortKey, rows] of rowsByCohort.entries()) {
    const ordered = [...rows].sort((a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime());
    for (let i = 0; i < ordered.length; i += 1) {
      const row = ordered[i];
      const forecastKey = `${cohortKey}::${row.bucketStartUtc.toISOString()}`;
      const coreForecast = coreForecastByKey.get(forecastKey);
      if (!coreForecast) continue;

      const regimeSuppressionFactor =
        row.state === "non-inferable" ? 0.18 : row.state === "churn-dominated" ? 0.4 : 1;
      const suppressedConsumption = (value: number) => clamp(0.48 + (value - 0.48) * regimeSuppressionFactor);

      const cohortCoverage = consumptionCoverageByCohort.get(cohortKey) ?? {
        usable12: 0,
        censored12: 0,
        usable24: 0,
        censored24: 0,
        usable72: 0,
        censored72: 0,
      };
      cohortCoverage.usable12 += row.consumptionLabelSummaryByHorizon[12].usableCount;
      cohortCoverage.censored12 += row.consumptionLabelSummaryByHorizon[12].censoredCount;
      cohortCoverage.usable24 += row.consumptionLabelSummaryByHorizon[24].usableCount;
      cohortCoverage.censored24 += row.consumptionLabelSummaryByHorizon[24].censoredCount;
      cohortCoverage.usable72 += row.consumptionLabelSummaryByHorizon[72].usableCount;
      cohortCoverage.censored72 += row.consumptionLabelSummaryByHorizon[72].censoredCount;
      consumptionCoverageByCohort.set(cohortKey, cohortCoverage);

      for (const horizonHours of CONSUMPTION_HORIZONS) {
        const targetTs = row.bucketStartUtc.getTime() + horizonHours * 3600 * 1000;
        const futureIndex = ordered.findIndex((candidate) => candidate.bucketStartUtc.getTime() >= targetTs);
        const future = futureIndex >= 0 ? ordered[futureIndex] : null;
        if (!future) continue;
        const sustainedWindow = ordered.slice(futureIndex, futureIndex + 3);
        const realizedTight = future.state === "tight" || future.state === "tightening";
        const realizedTightSustained =
          sustainedWindow.filter(
            (candidate) => candidate.state === "tight" || candidate.state === "tightening",
          ).length >= 2;
        const realizedPriceUp =
          future.medianPrice != null && row.medianPrice != null && future.medianPrice > row.medianPrice;

        const predictedPTight =
          horizonHours === 12 ? coreForecast.pTight24h : horizonHours === 24 ? coreForecast.pTight24h : coreForecast.pTight72h;
        const predictedPPriceUp = coreForecast.pPriceUp24h * (horizonHours === 72 ? 0.9 : 1);
        const predictedConsumptionProbRaw = suppressedConsumption(
          estimateConsumptionProbability({
            cohortState: row.state,
            relativePricePosition: 0,
            reliabilityScore: null,
            pressure: row.cohortPressureScore,
            hours: horizonHours,
            signalStrengthScore: row.signalStrengthScore,
            inferabilityScore: row.inferabilityScore,
          }),
        );
        const legacyRealizedConsumption =
          (future.totalOffers < row.totalOffers &&
            (future.persistentDisappearanceRateN ?? future.persistentDisappearanceRate) >=
              (row.persistentDisappearanceRateN ?? row.persistentDisappearanceRate)) ||
          (future.persistentDisappearanceRateN ?? future.persistentDisappearanceRate) -
            (row.persistentDisappearanceRateN ?? row.persistentDisappearanceRate) >
            0.03;

        const calibrationBucket = `${Math.floor(predictedConsumptionProbRaw * 10) / 10}-${Math.min(1, Math.floor(predictedConsumptionProbRaw * 10) / 10 + 0.1).toFixed(1)}`;
        for (const eventLabel of row.consumptionEventLabels) {
          const consumed =
            horizonHours === 12
              ? eventLabel.consumedWithin12h
              : horizonHours === 24
                ? eventLabel.consumedWithin24h
                : eventLabel.consumedWithin72h;
          const censored =
            horizonHours === 12
              ? eventLabel.censoredWithin12h
              : horizonHours === 24
                ? eventLabel.censoredWithin24h
                : eventLabel.censoredWithin72h;

          if (censored) {
            consumptionCensoredCountsByHorizon.set(
              horizonHours,
              (consumptionCensoredCountsByHorizon.get(horizonHours) ?? 0) + 1,
            );
          } else {
            consumptionUsableCountsByHorizon.set(
              horizonHours,
              (consumptionUsableCountsByHorizon.get(horizonHours) ?? 0) + 1,
            );
          }
          const cohortHorizonSummary = row.consumptionLabelSummaryByHorizon[horizonHours];
          const consumptionLabelQuality: PendingBacktestRow["consumptionLabelQuality"] = censored
            ? "censored"
            : cohortHorizonSummary.usableCount < 5
              ? "under-observed"
              : eventLabel.timeToReappearanceBuckets != null &&
                  eventLabel.timeToReappearanceBuckets <= SHORT_GAP_MAX_BUCKETS
                ? "ambiguous-reappearance"
                : "usable";

          pendingRows.push({
            source: row.key.source,
            gpuName: row.key.gpuName,
            numGpus: row.key.numGpus,
            offerType: row.key.offerType,
            predictionBucketStartUtc: row.bucketStartUtc,
            horizonHours,
            predictedPTight,
            realizedTight,
            predictedPPriceUp,
            realizedPriceUp,
            predictedConsumptionProbRaw,
            predictedConsumptionProbCalibrated: predictedConsumptionProbRaw,
            predictedConsumptionProb: predictedConsumptionProbRaw,
            realizedConsumption: consumed ?? false,
            realizedConsumptionLegacy: legacyRealizedConsumption,
            consumptionLabelCensored: censored,
            consumptionLabelQuality,
            timeToReappearanceBuckets: eventLabel.timeToReappearanceBuckets,
            timeToReappearanceHours: eventLabel.timeToReappearanceHours,
            realizedConsumedWithin12h: eventLabel.consumedWithin12h,
            realizedConsumedWithin24h: eventLabel.consumedWithin24h,
            realizedConsumedWithin72h: eventLabel.consumedWithin72h,
            realizedTightSustained,
            confidenceBucket: bucketizeScore(row.stateConfidence),
            inferabilityBucket: bucketizeScore(row.inferabilityScore),
            stateAtPrediction: row.state,
            calibrationBucket,
            modelVersion: MODEL_VERSION,
          });
        }
      }
    }
  }

  const calibrationMetadata: Record<string, ReturnType<typeof buildEmpiricalCalibrator>> = {};
  for (const horizonHours of CONSUMPTION_HORIZONS) {
    const calibrationRows = pendingRows.filter(
      (row) => row.horizonHours === horizonHours && !row.consumptionLabelCensored,
    );
    const calibrator = buildEmpiricalCalibrator(
      calibrationRows.map((row) => ({
        predicted: row.predictedConsumptionProbRaw,
        realized: row.realizedConsumption,
      })),
      { step: CALIBRATION_BUCKET_STEP, minCount: 24, priorWeight: 10 },
    );
    calibrationMetadata[String(horizonHours)] = calibrator;
    for (const row of pendingRows) {
      if (row.horizonHours !== horizonHours) continue;
      const calibrated = calibrator.calibrate(row.predictedConsumptionProbRaw);
      row.predictedConsumptionProbCalibrated = calibrated;
      row.predictedConsumptionProb = calibrated;
    }
  }

  if (pendingRows.length > 0) {
    const chunkSize = 5000;
    for (let offset = 0; offset < pendingRows.length; offset += chunkSize) {
      const chunk = pendingRows.slice(offset, offset + chunkSize);
      await prisma.forecastBacktest.createMany({
        data: chunk.map((row) => ({
          source: row.source,
          gpuName: row.gpuName,
          numGpus: row.numGpus,
          offerType: row.offerType,
          predictionBucketStartUtc: row.predictionBucketStartUtc,
          horizonHours: row.horizonHours,
          predictedPTight: row.predictedPTight,
          realizedTight: row.realizedTight,
          predictedPPriceUp: row.predictedPPriceUp,
          realizedPriceUp: row.realizedPriceUp,
          predictedConsumptionProb: row.predictedConsumptionProb,
          predictedConsumptionProbRaw: row.predictedConsumptionProbRaw,
          predictedConsumptionProbCalibrated: row.predictedConsumptionProbCalibrated,
          realizedConsumption: row.realizedConsumption,
          realizedConsumptionLegacy: row.realizedConsumptionLegacy,
          consumptionLabelCensored: row.consumptionLabelCensored,
          consumptionLabelQuality: row.consumptionLabelQuality,
          timeToReappearanceBuckets: row.timeToReappearanceBuckets,
          timeToReappearanceHours: row.timeToReappearanceHours,
          realizedConsumedWithin12h: row.realizedConsumedWithin12h,
          realizedConsumedWithin24h: row.realizedConsumedWithin24h,
          realizedConsumedWithin72h: row.realizedConsumedWithin72h,
          realizedTightSustained: row.realizedTightSustained,
          confidenceBucket: row.confidenceBucket,
          inferabilityBucket: row.inferabilityBucket,
          stateAtPrediction: row.stateAtPrediction,
          calibrationBucket: row.calibrationBucket,
          modelVersion: row.modelVersion,
        })),
      });
    }
  }

  await mkdir(CALIBRATION_DIR, { recursive: true });
  await writeFile(
    CALIBRATION_FILE,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        modelVersion: MODEL_VERSION,
        calibrationVersion: CALIBRATION_VERSION,
        horizons: Object.fromEntries(
          CONSUMPTION_HORIZONS.map((horizonHours) => {
            const calibrator = calibrationMetadata[String(horizonHours)];
            return [
              String(horizonHours),
              {
                globalRate: calibrator.globalRate,
                step: calibrator.step,
                buckets: calibrator.buckets,
              },
            ];
          }),
        ),
      },
      null,
      2,
    ),
  );

  await writeFile(
    VALIDATION_FILE,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        modelVersion: MODEL_VERSION,
        calibrationVersion: CALIBRATION_VERSION,
        consumptionCounts: {
          usableByHorizon: Object.fromEntries(CONSUMPTION_HORIZONS.map((h) => [h, consumptionUsableCountsByHorizon.get(h) ?? 0])),
          censoredByHorizon: Object.fromEntries(CONSUMPTION_HORIZONS.map((h) => [h, consumptionCensoredCountsByHorizon.get(h) ?? 0])),
        },
        coverageByCohort: [...consumptionCoverageByCohort.entries()].map(([cohort, coverage]) => ({
          cohort,
          ...coverage,
        })),
      },
      null,
      2,
    ),
  );

  console.log(`Upserted ${bucketRows.length} lifecycle-aware trend aggregate rows.`);
  console.log(`Upserted ${lifecycleMap.size} offer lifecycle rows.`);
  console.log(`Upserted ${(await prisma.cohortForecast.count())} cohort forecast rows.`);
  console.log(`Upserted ${(await prisma.forecastBacktest.count())} forecast backtest rows.`);
  if (rollupSummary) {
    console.log(`Refreshed ${rollupSummary.groupCount} latest rollups from snapshot ${rollupSummary.snapshotId}.`);
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

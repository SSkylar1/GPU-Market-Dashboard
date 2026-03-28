import "dotenv/config";
import { PrismaClient, type Offer } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { floorToUtcHalfHour } from "../src/lib/metrics/aggregation";
import { classifyDisappearanceOutcome, summarizeReappearanceGaps } from "../src/lib/metrics/transitions";
import {
  classifyCohortState,
  computeCohortPressureScore,
  computeNoiseScore,
  computeStateConfidence,
  forecastProbabilitiesFromState,
  mean,
  percentile,
  safeDiv,
  shrinkTowardsFamily,
  type CohortState,
  type TrendPoint,
} from "../src/lib/metrics/intelligence";
import { buildOfferIdentity, type OfferIdentityResult } from "../src/lib/metrics/offerIdentity";

const PERSISTENCE_BUCKETS = Number(process.env.PERSISTENCE_BUCKETS ?? 3);
const SHORT_GAP_MAX_BUCKETS = Number(process.env.SHORT_GAP_MAX_BUCKETS ?? 2);
const MODEL_VERSION = "predictive-v3";
const CALIBRATION_VERSION = "cal-v1";

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
  dataDepthScore: number;
  noiseScore: number;
  churnScore: number;
  signalStrengthScore: number;
  inferabilityScore: number;
  identityQualityScore: number;
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
  totalVisibleHours: number;
  disappearanceCount: number;
  reappearanceCount: number;
  longestContinuousVisibleHours: number;
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
  if (
    offer.stableOfferFingerprint &&
    offer.stableOfferFingerprint.trim().length > 0 &&
    offer.versionFingerprint &&
    offer.versionFingerprint.trim().length > 0
  ) {
    return {
      stableOfferFingerprint: offer.stableOfferFingerprint,
      versionFingerprint: offer.versionFingerprint,
      strategy: normalizeIdentityStrategy(offer.identityStrategy),
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
    reliabilityScore: offer.reliabilityScore,
    verified: offer.verified,
    pricePerHour: offer.pricePerHour,
    inetDownMbps: offer.inetDownMbps,
    inetUpMbps: offer.inetUpMbps,
  });

  return {
    stableOfferFingerprint: inferred.stableOfferFingerprint,
    versionFingerprint: inferred.versionFingerprint,
    strategy: inferred.strategy,
    identityQualityScore: inferred.identityQualityScore,
    offerExternalId: inferred.offerExternalId,
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

  const snapshots = await prisma.marketSnapshot.findMany({
    orderBy: { capturedAt: "asc" },
    include: { offers: true },
  });

  if (snapshots.length === 0) {
    console.log("No snapshots available for recompute.");
    return;
  }

  const lifecycleMap = new Map<string, LifecycleWorking>();
  const bucketsByKey = new Map<string, CohortBucket>();
  const historyByCohort = new Map<string, TrendPoint[]>();

  const snapshotsBySource = new Map<string, typeof snapshots>();
  for (const snapshot of snapshots) {
    const current = snapshotsBySource.get(snapshot.source) ?? [];
    current.push(snapshot);
    snapshotsBySource.set(snapshot.source, current);
  }

  for (const [source, sourceSnapshots] of snapshotsBySource.entries()) {
    const sortedSnapshots = [...sourceSnapshots].sort(
      (a, b) => a.capturedAt.getTime() - b.capturedAt.getTime(),
    );

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
          const stableFp = inferOfferIdentity(futureOffer, source).stableOfferFingerprint;

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
        const identity = inferOfferIdentity(offer, source);
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
            totalVisibleHours: 0,
            disappearanceCount: 0,
            reappearanceCount: 0,
            longestContinuousVisibleHours: 0,
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
            existing.currentSegmentStart = snapshot.capturedAt;
            existing.currentSegmentPrices = [];
            existing.currentSegmentStartPrice = offer.pricePerHour;
            existing.currentSegmentStartRentable = offer.rentable;
          }

          existing.totalVisibleSnapshots += 1;
          existing.totalVisibleHours += Math.max(0, Math.min(hoursSinceLastSeen, 1.5));
          existing.lastSeenAt = snapshot.capturedAt;
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
          prevMap.set(inferOfferIdentity(offer, source).stableOfferFingerprint, offer);
        }
        for (const offer of current) {
          currentMap.set(inferOfferIdentity(offer, source).stableOfferFingerprint, offer);
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
        const evaluableDisappearedOffers = Math.max(
          0,
          disappearedOffers - rightCensoredDisappearedOffers,
        );

        const priorCount = Math.max(prevSet.size, 1);

        const stats = priceStats(current);
        const prevStats = priceStats(prev);

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
          (mean(current.map((offer) => inferOfferIdentity(offer, source).identityQualityScore)) || 0) * 100;

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

        const dataDepthScore =
          100 *
          Math.min(
            1,
            0.55 * safeDiv(current.length, 50) +
              0.25 * safeDiv(currMachineSet.size, 20) +
              0.2 * safeDiv(priorPressureSeries.length, 48),
          );

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
          dataDepthScore,
          noiseScore,
          churnScore: pressure.churnScore,
          signalStrengthScore: adjustedSignalStrengthScore,
          inferabilityScore: adjustedInferabilityScore,
          identityQualityScore,
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
        dataDepthScore: bucket.dataDepthScore,
        noiseScore: bucket.noiseScore,
        churnScore: bucket.churnScore,
        signalStrengthScore: bucket.signalStrengthScore,
        inferabilityScore: bucket.inferabilityScore,
        identityQualityScore: bucket.identityQualityScore,
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
        totalVisibleHours: lifecycle.totalVisibleHours,
        disappearanceCount: lifecycle.disappearanceCount,
        reappearanceCount: lifecycle.reappearanceCount,
        longestContinuousVisibleHours: lifecycle.longestContinuousVisibleHours,
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

  const forecastsByKey = new Map<string, Array<{ horizonHours: number; pTight: number; pPriceUp: number }>>();

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

      const fKey = `${makeCohortKey(bucket.key)}::${bucket.bucketStartUtc.toISOString()}`;
      const current = forecastsByKey.get(fKey) ?? [];
      current.push({ horizonHours: horizon, pTight, pPriceUp });
      forecastsByKey.set(fKey, current);
    }
  }

  await prisma.forecastBacktest.deleteMany({});

  const rowsByCohort = new Map<string, CohortBucket[]>();
  for (const row of bucketRows) {
    const key = makeCohortKey(row.key);
    const current = rowsByCohort.get(key) ?? [];
    current.push(row);
    rowsByCohort.set(key, current);
  }

  for (const [cohortKey, rows] of rowsByCohort.entries()) {
    const ordered = [...rows].sort((a, b) => a.bucketStartUtc.getTime() - b.bucketStartUtc.getTime());
    for (let i = 0; i < ordered.length; i += 1) {
      const row = ordered[i];
      const forecastKey = `${cohortKey}::${row.bucketStartUtc.toISOString()}`;
      const forecasts = forecastsByKey.get(forecastKey) ?? [];
      for (const forecast of forecasts) {
        const targetTs = row.bucketStartUtc.getTime() + forecast.horizonHours * 3600 * 1000;
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
        const realizedConsumption =
          (future.totalOffers < row.totalOffers &&
            (future.persistentDisappearanceRateN ?? future.persistentDisappearanceRate) >=
              (row.persistentDisappearanceRateN ?? row.persistentDisappearanceRate)) ||
          (future.persistentDisappearanceRateN ?? future.persistentDisappearanceRate) -
            (row.persistentDisappearanceRateN ?? row.persistentDisappearanceRate) >
            0.03;

        const calibrationBucket = `${Math.floor(forecast.pTight * 10) / 10}-${Math.min(1, Math.floor(forecast.pTight * 10) / 10 + 0.1).toFixed(1)}`;

        await prisma.forecastBacktest.create({
          data: {
            source: row.key.source,
            gpuName: row.key.gpuName,
            numGpus: row.key.numGpus,
            offerType: row.key.offerType,
            predictionBucketStartUtc: row.bucketStartUtc,
            horizonHours: forecast.horizonHours,
            predictedPTight: forecast.pTight,
            realizedTight,
            predictedPPriceUp: forecast.pPriceUp,
            realizedPriceUp,
            predictedConsumptionProb: row.persistentDisappearanceRateN,
            realizedConsumption,
            realizedTightSustained,
            confidenceBucket: bucketizeScore(row.stateConfidence),
            inferabilityBucket: bucketizeScore(row.inferabilityScore),
            stateAtPrediction: row.state,
            calibrationBucket,
            modelVersion: MODEL_VERSION,
          },
        });
      }
    }
  }

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

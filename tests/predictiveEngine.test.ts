import { test } from "node:test";
import assert from "node:assert/strict";
import { z } from "zod";
import {
  brierScore,
  buildCalibrationBuckets,
  computeCohortPressureScore,
  estimateExpectedUtilization,
  estimateRoiContext,
  shrinkTowardsFamily,
} from "../src/lib/metrics/intelligence";
import { buildOfferIdentity, detectFingerprintCollisions } from "../src/lib/metrics/offerIdentity";
import { filterSeriesByRangeHours } from "../src/lib/scoring/chartTransforms";
import {
  classifyDisappearanceOutcome,
  computeSnapshotTransition,
  segmentLifecyclePresence,
  summarizeReappearanceGaps,
} from "../src/lib/metrics/transitions";

test("offer fingerprint continuity keeps stable identity across snapshots", () => {
  const first = buildOfferIdentity({
    source: "vast-live",
    offerExternalId: null,
    machineId: 42,
    hostId: 7,
    gpuName: "NVIDIA L4",
    numGpus: 2,
    offerType: "on-demand",
    gpuRamGb: 24,
    cpuCores: 16,
    ramGb: 64,
    reliabilityScore: 0.987,
    verified: true,
    pricePerHour: 1.42,
    inetDownMbps: 900,
    inetUpMbps: 800,
  });

  const second = buildOfferIdentity({
    source: "vast-live",
    offerExternalId: null,
    machineId: 42,
    hostId: 7,
    gpuName: "NVIDIA L4",
    numGpus: 2,
    offerType: "on-demand",
    gpuRamGb: 24,
    cpuCores: 16,
    ramGb: 64,
    reliabilityScore: 0.987,
    verified: true,
    pricePerHour: 1.42,
    inetDownMbps: 900,
    inetUpMbps: 800,
  });

  assert.equal(first.stableOfferFingerprint, second.stableOfferFingerprint);
  assert.notEqual(first.versionFingerprint, "");
});

test("price edit mutates version fingerprint but not stable fingerprint", () => {
  const first = buildOfferIdentity({
    source: "vast-live",
    machineId: 42,
    hostId: 7,
    gpuName: "NVIDIA L4",
    numGpus: 2,
    offerType: "on-demand",
    gpuRamGb: 24,
    pricePerHour: 1.2,
  });
  const second = buildOfferIdentity({
    source: "vast-live",
    machineId: 42,
    hostId: 7,
    gpuName: "NVIDIA L4",
    numGpus: 2,
    offerType: "on-demand",
    gpuRamGb: 24,
    pricePerHour: 1.6,
  });
  assert.equal(first.stableOfferFingerprint, second.stableOfferFingerprint);
  assert.notEqual(first.versionFingerprint, second.versionFingerprint);
});

test("fingerprint collisions are surfaced", () => {
  const collisions = detectFingerprintCollisions([
    { fingerprint: "f1", signature: "a" },
    { fingerprint: "f1", signature: "b" },
    { fingerprint: "f2", signature: "x" },
  ]);

  assert.equal(collisions.size, 1);
  assert.ok(collisions.get("f1")?.has("a"));
  assert.ok(collisions.get("f1")?.has("b"));
});

test("pressure score is monotonic for tighter conditions", () => {
  const weak = computeCohortPressureScore({
    persistentDisappearanceRate: 0.05,
    disappearedRate: 0.08,
    newOfferRate: 0.3,
    lowBandPersistentDisappearedRate: 0.05,
    medianPriceChangePct: -0.02,
    rentableShare: 0.75,
    uniqueMachineCount: 4,
    machineConcentrationShareTop3: 0.9,
    reappearedRate: 0.28,
    temporaryMissingRate: 0.22,
    identityQualityScore: 55,
    priorPressure: 35,
    priorPressure2: 30,
  });

  const strong = computeCohortPressureScore({
    persistentDisappearanceRate: 0.28,
    disappearedRate: 0.33,
    newOfferRate: 0.04,
    lowBandPersistentDisappearedRate: 0.35,
    medianPriceChangePct: 0.05,
    rentableShare: 0.25,
    uniqueMachineCount: 22,
    machineConcentrationShareTop3: 0.35,
    reappearedRate: 0.06,
    temporaryMissingRate: 0.04,
    identityQualityScore: 92,
    priorPressure: 62,
    priorPressure2: 58,
  });

  assert.ok(strong.cohortPressureScore > weak.cohortPressureScore);
});

test("pairwise snapshot transitions and reappearance detection", () => {
  const transition = computeSnapshotTransition({
    previousIds: new Set(["a", "b", "c"]),
    currentIds: new Set(["b", "c", "d", "x"]),
    seenBefore: new Set(["a", "b", "c", "x"]),
  });

  assert.deepEqual(new Set([...transition.continuing]), new Set(["b", "c"]));
  assert.deepEqual(new Set([...transition.added]), new Set(["d", "x"]));
  assert.deepEqual(new Set([...transition.disappeared]), new Set(["a"]));
  assert.deepEqual(new Set([...transition.reappeared]), new Set(["x"]));
});

test("lifecycle segmentation splits continuous visibility windows", () => {
  const base = new Date("2026-03-28T00:00:00.000Z");
  const segments = segmentLifecyclePresence([
    { at: new Date(base.getTime() + 0 * 3600 * 1000), visible: true },
    { at: new Date(base.getTime() + 1 * 3600 * 1000), visible: true },
    { at: new Date(base.getTime() + 2 * 3600 * 1000), visible: false },
    { at: new Date(base.getTime() + 3 * 3600 * 1000), visible: true },
    { at: new Date(base.getTime() + 4 * 3600 * 1000), visible: true },
  ]);

  assert.equal(segments.length, 2);
  assert.equal(segments[0].endedBy, "disappeared");
  assert.equal(segments[1].endedBy, "still_active");
});

test("multi-bucket disappearance logic distinguishes persistent vs short/long gap", () => {
  const shortGap = classifyDisappearanceOutcome({
    id: "a",
    futureBuckets: [new Set<string>(), new Set<string>(["a"])],
    shortGapMaxBuckets: 2,
  });
  const longGap = classifyDisappearanceOutcome({
    id: "a",
    futureBuckets: [new Set<string>(), new Set<string>(), new Set<string>(["a"])],
    shortGapMaxBuckets: 2,
  });
  const persistent = classifyDisappearanceOutcome({
    id: "a",
    futureBuckets: [new Set<string>(), new Set<string>(), new Set<string>()],
    shortGapMaxBuckets: 2,
  });
  const summary = summarizeReappearanceGaps([1, 2, 4], 2);
  assert.equal(shortGap, "reappeared_short_gap");
  assert.equal(longGap, "reappeared_long_gap");
  assert.equal(persistent, "persistently_disappeared");
  assert.equal(summary.medianDelayBuckets, 2);
  assert.ok(summary.shortGapReappearanceRate > summary.longGapReappearanceRate);
});

test("config-family shrinkage behaves as expected", () => {
  const thin = shrinkTowardsFamily(80, 40, 3);
  const deep = shrinkTowardsFamily(80, 40, 160);
  assert.ok(thin < deep);
  assert.ok(thin > 40);
  assert.ok(deep < 80);
});

test("scenario economics sanity and payback behavior", () => {
  const utilization = estimateExpectedUtilization({
    cohortState: "tight",
    pressure: 78,
    relativePricePosition: -0.03,
    reliabilityScore: 0.985,
    machineDepthScore: 68,
    concentrationScore: 60,
    configVsFamilyHazardDelta: 4,
    confidenceScore: 74,
    inferabilityScore: 72,
    signalStrengthScore: 76,
    churnScore: 22,
  });

  if (typeof utilization === "number") {
    assert.fail("Expected utilization distribution object for distribution input");
  }

  const economics = estimateRoiContext({
    utilization,
    listingPricePerHour: 1.4,
    hardwareCost: 4800,
    powerWatts: 700,
    electricityCostPerKwh: 0.11,
    targetPaybackMonths: 16,
  });

  assert.ok(economics.expectedDailyRevenue > 0);
  assert.ok(economics.expectedDailyRevenueHigh >= economics.expectedDailyRevenueLow);
  assert.ok(economics.pPaybackWithinTarget >= 0 && economics.pPaybackWithinTarget <= 1);
});

test("calibration metric calculations", () => {
  const items = [
    { predicted: 0.8, realized: true },
    { predicted: 0.6, realized: false },
    { predicted: 0.2, realized: false },
    { predicted: 0.9, realized: true },
  ];

  const brier = brierScore(items);
  const buckets = buildCalibrationBuckets(items, 0.2);

  assert.ok(brier >= 0);
  assert.ok(buckets.length > 0);
  assert.ok(buckets.some((bucket) => bucket.count > 0));
});

test("API response shape validation for predictive payload", () => {
  const schema = z.object({
    recommendation: z.string(),
    forecastSuppressed: z.boolean(),
    vetoReason: z.string().nullable(),
    currentState: z.object({
      state: z.string(),
      pressure: z.number(),
      movementScore: z.number(),
      confidenceScore: z.number(),
      inferabilityScore: z.number(),
      signalStrengthScore: z.number(),
      identityQualityScore: z.number(),
    }),
    forecastProbabilities: z.object({
      pTight24h: z.number(),
      pTight72h: z.number(),
      pTight7d: z.number(),
    }),
    economics: z.object({
      expectedPaybackMonths: z.number().nullable(),
      pPaybackWithinTarget: z.number(),
    }),
    visuals: z.object({
      pressureTimeline: z.array(z.object({ bucketStartUtc: z.string(), pressure: z.number() })),
      calibration: z.array(z.object({ bucket: z.string(), avgPredicted: z.number(), realizedRate: z.number() })),
      pricePositionCurve: z.array(z.object({ relativePricePosition: z.number(), p24hLow: z.number(), p24hHigh: z.number() })),
    }),
  });

  const parsed = schema.safeParse({
    recommendation: "Watch",
    forecastSuppressed: false,
    vetoReason: null,
    currentState: { state: "balanced", pressure: 53, movementScore: 44, confidenceScore: 61, inferabilityScore: 58, signalStrengthScore: 57, identityQualityScore: 73 },
    forecastProbabilities: { pTight24h: 0.41, pTight72h: 0.39, pTight7d: 0.34 },
    economics: { expectedPaybackMonths: 18.2, pPaybackWithinTarget: 0.46 },
    visuals: {
      pressureTimeline: [{ bucketStartUtc: "2026-03-28T12:00:00.000Z", pressure: 53 }],
      pricePositionCurve: [{ relativePricePosition: 0, p24hLow: 0.4, p24hHigh: 0.6 }],
      calibration: [{ bucket: "0.4-0.5", avgPredicted: 0.44, realizedRate: 0.41 }],
    },
  });

  assert.equal(parsed.success, true);
});

test("UI chart range transform keeps trailing window", () => {
  const series = [
    { bucketStartUtc: "2026-03-27T00:00:00.000Z", value: 1 },
    { bucketStartUtc: "2026-03-27T12:00:00.000Z", value: 2 },
    { bucketStartUtc: "2026-03-28T00:00:00.000Z", value: 3 },
  ];

  const filtered = filterSeriesByRangeHours(series, 18);
  assert.equal(filtered.length, 2);
  assert.equal(filtered[0].value, 2);
});

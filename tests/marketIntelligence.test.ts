import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildRecommendation,
  classifyCohortState,
  classifyMarketRegime,
  computeCohortPressureScore,
  computeWindowTrendSummary,
  deriveMarketPressureFromPair,
  estimateConsumptionProbability,
  estimateExpectedUtilization,
  estimateRoiContext,
} from "../src/lib/metrics/intelligence";

test("deriveMarketPressureFromPair returns stable 0..100 components", () => {
  const pressure = deriveMarketPressureFromPair({
    newOfferRate: 0.1,
    disappearedRate: 0.4,
    medianPriceChange: 0.2,
    priorMedianPrice: 1,
    rentableShareChange: -0.2,
  });

  assert.ok(pressure.marketPressureScore >= 0 && pressure.marketPressureScore <= 100);
  assert.ok(pressure.marketPressureChurnComponent >= 0 && pressure.marketPressureChurnComponent <= 100);
  assert.ok(pressure.marketPressureSupplyComponent >= 0 && pressure.marketPressureSupplyComponent <= 100);
  assert.ok(pressure.marketPressurePriceComponent >= 0 && pressure.marketPressurePriceComponent <= 100);
  assert.ok(
    pressure.marketPressureAvailabilityComponent >= 0 &&
      pressure.marketPressureAvailabilityComponent <= 100,
  );
});

test("computeWindowTrendSummary handles sparse history", () => {
  const points = [
    {
      bucketStartUtc: new Date("2026-03-21T00:00:00.000Z"),
      totalOffers: 10,
      rentableOffers: 6,
      rentedOffers: 1,
      minPrice: 0.4,
      p10Price: 0.45,
      medianPrice: 0.6,
      p90Price: 0.9,
      newOfferCount: 1,
      disappearedOfferCount: 2,
      newOfferRate: 0.1,
      disappearedRate: 0.2,
      netSupplyChange: -1,
      medianPriceChange: 0.05,
      rentableShareChange: -0.1,
      marketPressureScore: 62,
      marketPressurePriceComponent: 55,
      marketPressureChurnComponent: 70,
      marketPressureSupplyComponent: 61,
      marketPressureAvailabilityComponent: 60,
      lowBandDisappearedCount: 1,
      midBandDisappearedCount: 1,
      highBandDisappearedCount: 0,
      lowBandDisappearedRate: 0.5,
      midBandDisappearedRate: 0.33,
      highBandDisappearedRate: 0,
    },
  ];

  const summary = computeWindowTrendSummary(points, new Date("2026-03-21T00:00:00.000Z"), "6h");
  assert.equal(summary.pointCount, 1);
  assert.equal(summary.supply?.latest, 10);
  assert.equal(summary.marketPressureScore?.latest, 62);
});

test("classifyMarketRegime returns tight and oversupplied states", () => {
  const tight = classifyMarketRegime({
    disappearedRate: 0.3,
    newOfferRate: 0.1,
    netSupplyChange: -2,
    medianPriceChange: 0.02,
    rentableShareChange: -0.05,
    marketPressureScore: 75,
  });

  const oversupplied = classifyMarketRegime({
    disappearedRate: 0.05,
    newOfferRate: 0.4,
    netSupplyChange: 4,
    medianPriceChange: -0.03,
    rentableShareChange: 0.08,
    marketPressureScore: 30,
  });

  assert.equal(tight, "tight");
  assert.equal(oversupplied, "oversupplied");
});

test("classifyCohortState detects churn-dominated and non-inferable", () => {
  const churn = classifyCohortState({
    dataDepthScore: 48,
    noiseScore: 35,
    cohortPressureScore: 54,
    pressureAcceleration: 1,
    persistentDisappearanceRate: 0.08,
    reappearedRate: 0.4,
    churnScore: 75,
    signalStrengthScore: 34,
    inferabilityScore: 42,
    identityQualityScore: 65,
  });
  const nonInferable = classifyCohortState({
    dataDepthScore: 18,
    noiseScore: 55,
    cohortPressureScore: 50,
    pressureAcceleration: 0,
    persistentDisappearanceRate: 0.05,
    reappearedRate: 0.2,
    churnScore: 40,
    signalStrengthScore: 25,
    inferabilityScore: 24,
    identityQualityScore: 32,
  });
  assert.equal(churn, "churn-dominated");
  assert.equal(nonInferable, "non-inferable");
});

test("weak signal flattens price sensitivity", () => {
  const strongCheap = estimateConsumptionProbability({
    cohortState: "balanced",
    relativePricePosition: -0.2,
    reliabilityScore: 0.98,
    pressure: 55,
    hours: 24,
    signalStrengthScore: 85,
    inferabilityScore: 85,
  });
  const strongExpensive = estimateConsumptionProbability({
    cohortState: "balanced",
    relativePricePosition: 0.2,
    reliabilityScore: 0.98,
    pressure: 55,
    hours: 24,
    signalStrengthScore: 85,
    inferabilityScore: 85,
  });
  const weakCheap = estimateConsumptionProbability({
    cohortState: "balanced",
    relativePricePosition: -0.2,
    reliabilityScore: 0.98,
    pressure: 55,
    hours: 24,
    signalStrengthScore: 20,
    inferabilityScore: 20,
  });
  const weakExpensive = estimateConsumptionProbability({
    cohortState: "balanced",
    relativePricePosition: 0.2,
    reliabilityScore: 0.98,
    pressure: 55,
    hours: 24,
    signalStrengthScore: 20,
    inferabilityScore: 20,
  });
  assert.ok(Math.abs(weakCheap - weakExpensive) < Math.abs(strongCheap - strongExpensive));
  assert.ok(weakCheap < 0.65);
});

test("recommendation veto blocks buy in non-inferable regime", () => {
  const reco = buildRecommendation({
    pPaybackWithinTarget: 0.91,
    expectedUtilization: 0.72,
    confidenceScore: 82,
    cohortState: "non-inferable",
    concentrationRisk: 0.2,
    downsideRisk: 0.1,
    inferabilityScore: 22,
    identityQualityScore: 30,
    churnScore: 70,
    signalStrengthScore: 20,
  });
  assert.equal(reco.recommendationLabel, "Avoid");
  assert.equal(reco.forecastSuppressed, true);
  assert.equal(reco.vetoReason, "non_inferable");
});

test("churn-heavy movement lowers signal/inferability relative to cleaner persistence", () => {
  const churnHeavy = computeCohortPressureScore({
    persistentDisappearanceRate: 0.1,
    disappearedRate: 0.6,
    newOfferRate: 0.5,
    lowBandPersistentDisappearedRate: 0.08,
    medianPriceChangePct: 0.01,
    rentableShare: 0.45,
    uniqueMachineCount: 10,
    machineConcentrationShareTop3: 0.5,
    reappearedRate: 0.7,
    temporaryMissingRate: 0.45,
    identityQualityScore: 85,
    priorPressure: 52,
    priorPressure2: 51,
  });
  const cleaner = computeCohortPressureScore({
    persistentDisappearanceRate: 0.45,
    disappearedRate: 0.5,
    newOfferRate: 0.1,
    lowBandPersistentDisappearedRate: 0.5,
    medianPriceChangePct: 0.04,
    rentableShare: 0.2,
    uniqueMachineCount: 14,
    machineConcentrationShareTop3: 0.35,
    reappearedRate: 0.08,
    temporaryMissingRate: 0.05,
    identityQualityScore: 85,
    priorPressure: 58,
    priorPressure2: 57,
  });
  assert.ok(churnHeavy.movementScore > cleaner.movementScore);
  assert.ok(churnHeavy.signalStrengthScore < cleaner.signalStrengthScore);
  assert.ok(churnHeavy.inferabilityScore < cleaner.inferabilityScore);
});

test("estimateExpectedUtilization and ROI handle non-positive margin", () => {
  const util = estimateExpectedUtilization({
    disappearedRate: 0,
    netSupplyChange: 3,
    visibleSupplyCount: 10,
    rentableShare: 0.9,
    listingPricePerHour: 0.2,
    medianPrice: 0.4,
    reliabilityScore: 0.91,
  });

  assert.ok(util >= 0 && util <= 0.98);

  const roi = estimateRoiContext({
    expectedUtilizationEstimate: util,
    listingPricePerHour: 0.2,
    hardwareCost: 2000,
    powerWatts: 3000,
    electricityCostPerKwh: 0.8,
  });

  assert.equal(roi.paybackPeriodDays, null);
});

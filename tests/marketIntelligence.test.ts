import { test } from "node:test";
import assert from "node:assert/strict";
import {
  classifyMarketRegime,
  computeWindowTrendSummary,
  deriveMarketPressureFromPair,
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

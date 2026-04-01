import { test } from "node:test";
import assert from "node:assert/strict";
import { buildConsumptionEventLabel } from "../src/lib/metrics/consumptionLabels";
import { buildEmpiricalCalibrator, combineDepthScores } from "../src/lib/metrics/intelligence";

test("delay-based consumption labels: short-gap relist is not consumed", () => {
  const label = buildConsumptionEventLabel({
    timeToReappearanceBuckets: 2,
    sourceBucketHours: 0.5,
    futureHoursAvailable: 80,
  });

  assert.equal(label.consumedWithin12h, false);
  assert.equal(label.consumedWithin24h, false);
  assert.equal(label.consumedWithin72h, false);
  assert.equal(label.censoredWithin12h, false);
  assert.equal(label.censoredWithin24h, false);
  assert.equal(label.censoredWithin72h, false);
});

test("delay-based consumption labels: absence beyond horizon is consumed", () => {
  const label = buildConsumptionEventLabel({
    timeToReappearanceBuckets: 160,
    sourceBucketHours: 0.5,
    futureHoursAvailable: 90,
  });

  assert.equal(label.consumedWithin12h, true);
  assert.equal(label.consumedWithin24h, true);
  assert.equal(label.consumedWithin72h, true);
});

test("delay-based consumption labels: insufficient future window is censored", () => {
  const label = buildConsumptionEventLabel({
    timeToReappearanceBuckets: null,
    sourceBucketHours: 0.5,
    futureHoursAvailable: 20,
  });

  assert.equal(label.consumedWithin12h, true);
  assert.equal(label.consumedWithin24h, null);
  assert.equal(label.consumedWithin72h, null);
  assert.equal(label.censoredWithin12h, false);
  assert.equal(label.censoredWithin24h, true);
  assert.equal(label.censoredWithin72h, true);
});

test("empirical calibrator maps raw predictions toward realized frequencies", () => {
  const calibrator = buildEmpiricalCalibrator(
    [
      { predicted: 0.8, realized: true },
      { predicted: 0.82, realized: true },
      { predicted: 0.85, realized: false },
      { predicted: 0.2, realized: false },
      { predicted: 0.22, realized: false },
      { predicted: 0.25, realized: false },
    ],
    { step: 0.1, minCount: 1, priorWeight: 1 },
  );

  const high = calibrator.calibrate(0.84);
  const low = calibrator.calibrate(0.22);
  assert.ok(high > low);
  assert.ok(calibrator.buckets.length > 0);
});

test("depth combination does not overstate tiny cross-sectional cohorts", () => {
  const highTimeLowCross = combineDepthScores(95, 8);
  const balancedDepth = combineDepthScores(70, 65);

  assert.ok(highTimeLowCross < 50);
  assert.ok(balancedDepth > highTimeLowCross);
});

import { test } from "node:test";
import assert from "node:assert/strict";
import { floorToUtcHalfHour, summarizeOffers } from "../src/lib/metrics/aggregation";

test("floorToUtcHalfHour maps to :00/:30 UTC buckets", () => {
  const d1 = new Date("2026-03-21T15:07:59.000Z");
  const d2 = new Date("2026-03-21T15:49:00.000Z");

  assert.equal(floorToUtcHalfHour(d1).toISOString(), "2026-03-21T15:00:00.000Z");
  assert.equal(floorToUtcHalfHour(d2).toISOString(), "2026-03-21T15:30:00.000Z");
});

test("summarizeOffers computes availability ratio and utilization", () => {
  const summary = summarizeOffers([
    { rentable: true, rented: false, pricePerHour: 0.1 },
    { rentable: false, rented: true, pricePerHour: 0.2 },
    { rentable: false, rented: true, pricePerHour: 0.3 },
  ]);

  assert.equal(summary.totalOffers, 3);
  assert.equal(summary.rentableOffers, 1);
  assert.equal(summary.rentedOffers, 2);
  assert.ok(Math.abs(summary.availabilityRatio - 1 / 3) < 1e-12);
  assert.ok(Math.abs(summary.impliedUtilization - 2 / 3) < 1e-12);
  assert.equal(summary.p10Price, 0.1);
  assert.equal(summary.medianPrice, 0.2);
  assert.equal(summary.p90Price, 0.3);
});

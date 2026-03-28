import "dotenv/config";
import { PrismaClient } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { brierScore, buildCalibrationBuckets, mean } from "../src/lib/metrics/intelligence";

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

async function main() {
  const rows = await prisma.forecastBacktest.findMany({
    orderBy: { predictionBucketStartUtc: "desc" },
    take: 10000,
  });

  if (rows.length === 0) {
    console.log("No backtest rows found.");
    return;
  }

  const tightItems = rows.map((row) => ({
    predicted: row.predictedPTight,
    realized: row.realizedTightSustained ?? row.realizedTight,
  }));
  const priceUpItems = rows.map((row) => ({
    predicted: row.predictedPPriceUp,
    realized: row.realizedPriceUp,
  }));
  const consumptionItems = rows.map((row) => ({
    predicted: row.predictedConsumptionProb,
    realized: row.realizedConsumption,
  }));

  const byGpu = new Map<string, typeof rows>();
  for (const row of rows) {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
    const current = byGpu.get(key) ?? [];
    current.push(row);
    byGpu.set(key, current);
  }

  const worstCalibrated = [...byGpu.entries()]
    .map(([key, cohortRows]) => ({
      key,
      count: cohortRows.length,
      brierTight: brierScore(
        cohortRows.map((row) => ({ predicted: row.predictedPTight, realized: row.realizedTight })),
      ),
      overconfidence: mean(
        cohortRows.map((row) => row.predictedPTight - (row.realizedTight ? 1 : 0)),
      ),
    }))
    .filter((entry) => entry.count >= 10)
    .sort((a, b) => b.brierTight - a.brierTight)
    .slice(0, 12);

  const calibrationBuckets = buildCalibrationBuckets(tightItems, 0.1);
  const breakdown = new Map<string, { count: number; avgPredicted: number; realized: number }>();
  for (const row of rows) {
    const key = `${row.horizonHours}h|state=${row.stateAtPrediction ?? "na"}|conf=${row.confidenceBucket ?? "na"}|infer=${row.inferabilityBucket ?? "na"}`;
    const current = breakdown.get(key) ?? { count: 0, avgPredicted: 0, realized: 0 };
    current.count += 1;
    current.avgPredicted += row.predictedPTight;
    current.realized += (row.realizedTightSustained ?? row.realizedTight) ? 1 : 0;
    breakdown.set(key, current);
  }

  console.log("=== Backtest Summary ===");
  console.log(`Rows: ${rows.length}`);
  console.log(`Brier (tight): ${brierScore(tightItems).toFixed(4)}`);
  console.log(`Brier (price up): ${brierScore(priceUpItems).toFixed(4)}`);
  console.log(`Brier (consumption): ${brierScore(consumptionItems).toFixed(4)}`);
  console.log("");

  console.log("=== Calibration Breakdown (horizon/state/conf/inferability) ===");
  for (const [key, value] of [...breakdown.entries()].sort((a, b) => b[1].count - a[1].count).slice(0, 40)) {
    console.log(
      `${key} count=${value.count} predicted=${(value.avgPredicted / value.count).toFixed(3)} realized=${(value.realized / value.count).toFixed(3)}`,
    );
  }
  console.log("");

  console.log("=== Calibration Buckets (tight) ===");
  for (const bucket of calibrationBuckets) {
    console.log(
      `${bucket.bucket} count=${bucket.count} predicted=${bucket.avgPredicted.toFixed(3)} realized=${bucket.realizedRate.toFixed(3)}`,
    );
  }
  console.log("");

  console.log("=== Worst Cohorts by Brier (tight) ===");
  for (const cohort of worstCalibrated) {
    console.log(
      `${cohort.key} count=${cohort.count} brier=${cohort.brierTight.toFixed(4)} overconfidence=${cohort.overconfidence.toFixed(4)}`,
    );
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

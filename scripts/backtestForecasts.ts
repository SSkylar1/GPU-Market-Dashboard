import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { PrismaClient } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { brierScore, buildCalibrationBuckets, safeDiv } from "../src/lib/metrics/intelligence";

const MODEL_VERSION = "predictive-v3.2";
const CALIBRATION_VERSION = "consumption-cal-v2";
const OUTPUT_DIR = "docs/artifacts";
const SCORECARD_JSON = `${OUTPUT_DIR}/validation-scorecard-v32-backtest.json`;
const SCORECARD_MD = "docs/validation-scorecard.md";
const COHORT_COMPARE_JSON = `${OUTPUT_DIR}/cohort-comparison-v32.json`;
const INFERABILITY_JSON = `${OUTPUT_DIR}/inferability-distribution-v32.json`;

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

function cohortKey(row: { source: string; gpuName: string; numGpus: number | null; offerType: string | null }) {
  return `${row.source}::${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
}

function brierOrNull(items: Array<{ predicted: number; realized: boolean | number }>): number | null {
  return items.length > 0 ? brierScore(items) : null;
}

function scoreToBin(score: number): "0" | "0-10" | "10-25" | "25-50" | "50-75" | "75-100" {
  if (score === 0) return "0";
  if (score < 10) return "0-10";
  if (score < 25) return "10-25";
  if (score < 50) return "25-50";
  if (score < 75) return "50-75";
  return "75-100";
}

function emptyInferabilityBins() {
  return {
    "0": 0,
    "0-10": 0,
    "10-25": 0,
    "25-50": 0,
    "50-75": 0,
    "75-100": 0,
  };
}

async function main() {
  const rows = await prisma.forecastBacktest.findMany({
    orderBy: { predictionBucketStartUtc: "desc" },
    take: 200000,
  });

  if (rows.length === 0) {
    console.log("No backtest rows found.");
    return;
  }

  const uniqueForTight = new Map<string, (typeof rows)[number]>();
  for (const row of rows) {
    const key = `${cohortKey(row)}::${row.predictionBucketStartUtc.toISOString()}::${row.horizonHours}`;
    if (!uniqueForTight.has(key)) uniqueForTight.set(key, row);
  }
  const structuralRows = [...uniqueForTight.values()];

  const tightItems = structuralRows.map((row) => ({
    predicted: row.predictedPTight,
    realized: row.realizedTightSustained ?? row.realizedTight,
  }));
  const priceUpItems = structuralRows.map((row) => ({
    predicted: row.predictedPPriceUp,
    realized: row.realizedPriceUp,
  }));

  const consumptionRows = rows.filter((row) => !row.consumptionLabelCensored && [12, 24, 72].includes(row.horizonHours));

  const consumptionByHorizon = [12, 24, 72].map((horizonHours) => {
    const horizonRows = consumptionRows.filter((row) => row.horizonHours === horizonHours);
    const rawItems = horizonRows.map((row) => ({
      predicted: row.predictedConsumptionProbRaw ?? row.predictedConsumptionProb,
      realized: row.realizedConsumption,
    }));
    const calibratedItems = horizonRows.map((row) => ({
      predicted: row.predictedConsumptionProbCalibrated ?? row.predictedConsumptionProb,
      realized: row.realizedConsumption,
    }));
    const legacyItems = horizonRows
      .filter((row) => row.realizedConsumptionLegacy != null)
      .map((row) => ({
        predicted: row.predictedConsumptionProbRaw ?? row.predictedConsumptionProb,
        realized: row.realizedConsumptionLegacy as boolean,
      }));

    return {
      horizonHours,
      usable: horizonRows.length,
      censored: rows.filter((row) => row.horizonHours === horizonHours && row.consumptionLabelCensored).length,
      brierRaw: brierOrNull(rawItems),
      brierCalibrated: brierOrNull(calibratedItems),
      brierLegacyProxy: brierOrNull(legacyItems),
      calibrationRaw: buildCalibrationBuckets(rawItems, 0.1),
      calibrationCalibrated: buildCalibrationBuckets(calibratedItems, 0.1),
    };
  });

  const byCohort = new Map<string, (typeof rows)>();
  for (const row of rows) {
    const key = cohortKey(row);
    const current = byCohort.get(key) ?? [];
    current.push(row);
    byCohort.set(key, current);
  }

  const latestTrendRows = await prisma.gpuTrendAggregate.findMany({
    where: { source: "vast-live" },
    orderBy: { bucketStartUtc: "desc" },
    take: 2500,
  });
  const latestTrendByCohort = new Map<string, (typeof latestTrendRows)[number]>();
  for (const row of latestTrendRows) {
    const key = cohortKey(row);
    if (!latestTrendByCohort.has(key)) latestTrendByCohort.set(key, row);
  }

  const cohortStats = [...byCohort.entries()].map(([key, cohortRows]) => {
    const usableRows = cohortRows.filter((row) => !row.consumptionLabelCensored);
    const trend = latestTrendByCohort.get(key);
    const summaryByHorizon = [12, 24, 72].map((horizonHours) => {
      const scope = usableRows.filter((row) => row.horizonHours === horizonHours);
      return {
        horizonHours,
        count: scope.length,
        brierRaw: brierOrNull(
          scope.map((row) => ({ predicted: row.predictedConsumptionProbRaw ?? row.predictedConsumptionProb, realized: row.realizedConsumption })),
        ),
        brierCalibrated: brierOrNull(
          scope.map((row) => ({ predicted: row.predictedConsumptionProbCalibrated ?? row.predictedConsumptionProb, realized: row.realizedConsumption })),
        ),
        calibration: buildCalibrationBuckets(
          scope.map((row) => ({ predicted: row.predictedConsumptionProbCalibrated ?? row.predictedConsumptionProb, realized: row.realizedConsumption })),
          0.1,
        ),
      };
    });

    return {
      key,
      state: trend?.state ?? "unknown",
      confidence: trend?.stateConfidence ?? 0,
      inferability: trend?.inferabilityScore ?? 0,
      churnScore: trend?.churnScore ?? 0,
      crossSectionDepthScore: trend?.crossSectionDepthScore ?? 0,
      timeDepthScore: trend?.timeDepthScore ?? 0,
      uniqueMachines: trend?.uniqueMachines ?? 0,
      usage: {
        usable12: cohortRows.filter((row) => row.horizonHours === 12 && !row.consumptionLabelCensored).length,
        censored12: cohortRows.filter((row) => row.horizonHours === 12 && row.consumptionLabelCensored).length,
        usable24: cohortRows.filter((row) => row.horizonHours === 24 && !row.consumptionLabelCensored).length,
        censored24: cohortRows.filter((row) => row.horizonHours === 24 && row.consumptionLabelCensored).length,
        usable72: cohortRows.filter((row) => row.horizonHours === 72 && !row.consumptionLabelCensored).length,
        censored72: cohortRows.filter((row) => row.horizonHours === 72 && row.consumptionLabelCensored).length,
      },
      summaryByHorizon,
      recommendationDistribution: (() => {
        const counts = new Map<string, number>();
        for (const row of cohortRows) {
          const bucket = row.stateAtPrediction ?? "unknown";
          counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
        }
        return [...counts.entries()].map(([bucket, count]) => ({ bucket, count }));
      })(),
    };
  });

  const churnHeavy = cohortStats
    .filter((cohort) => cohort.usage.usable24 >= 20)
    .sort((a, b) => b.churnScore - a.churnScore)[0] ?? null;
  const thinClean = cohortStats
    .filter((cohort) => cohort.uniqueMachines > 0 && cohort.uniqueMachines <= 4 && cohort.churnScore < 50)
    .sort((a, b) => a.crossSectionDepthScore - b.crossSectionDepthScore)[0] ?? null;
  const deeperHealthyPrimary = cohortStats
    .filter((cohort) => cohort.crossSectionDepthScore >= 55 && cohort.inferability >= 55)
    .sort((a, b) => b.crossSectionDepthScore - a.crossSectionDepthScore)[0] ?? null;
  const deeperHealthyFallback = cohortStats
    .slice()
    .sort((a, b) => b.crossSectionDepthScore - a.crossSectionDepthScore)
    .find((cohort) => cohort.key !== churnHeavy?.key && cohort.key !== thinClean?.key) ?? null;
  const deeperHealthy = deeperHealthyPrimary ?? deeperHealthyFallback;

  const representativeCohorts = [
    { role: "churn-heavy", cohort: churnHeavy },
    { role: "thin-cleaner", cohort: thinClean },
    { role: "healthier-deeper", cohort: deeperHealthy },
  ].filter((row): row is { role: string; cohort: NonNullable<typeof row.cohort> } => row.cohort != null);

  const inferabilityBins = emptyInferabilityBins();
  const inferabilityByRegime = new Map<string, typeof inferabilityBins>();
  for (const row of latestTrendByCohort.values()) {
    const bin = scoreToBin(row.inferabilityScore ?? 0);
    inferabilityBins[bin] += 1;
    const state = row.state ?? "unknown";
    const regimeBins = inferabilityByRegime.get(state) ?? emptyInferabilityBins();
    regimeBins[bin] += 1;
    inferabilityByRegime.set(state, regimeBins);
  }

  const recommendationOutcomeRows = await prisma.scenarioForecast.findMany({
    orderBy: { createdAt: "desc" },
    take: 4000,
    select: {
      recommendation: true,
      expectedUtilization: true,
      pPaybackWithinTarget: true,
      expectedPaybackMonths: true,
      confidenceScore: true,
    },
  });
  const recommendationOutcomes = new Map<
    string,
    { count: number; avgExpectedUtilization: number; avgPaybackHitProb: number; avgConfidence: number; paybackKnown: number; avgPaybackMonths: number }
  >();
  for (const row of recommendationOutcomeRows) {
    const key = row.recommendation;
    const current = recommendationOutcomes.get(key) ?? {
      count: 0,
      avgExpectedUtilization: 0,
      avgPaybackHitProb: 0,
      avgConfidence: 0,
      paybackKnown: 0,
      avgPaybackMonths: 0,
    };
    current.count += 1;
    current.avgExpectedUtilization += row.expectedUtilization;
    current.avgPaybackHitProb += row.pPaybackWithinTarget;
    current.avgConfidence += row.confidenceScore;
    if (row.expectedPaybackMonths != null) {
      current.paybackKnown += 1;
      current.avgPaybackMonths += row.expectedPaybackMonths;
    }
    recommendationOutcomes.set(key, current);
  }

  const recommendationOutcomeSummary = [...recommendationOutcomes.entries()]
    .map(([recommendation, value]) => ({
      recommendation,
      count: value.count,
      avgExpectedUtilization: safeDiv(value.avgExpectedUtilization, value.count),
      avgPaybackHitProb: safeDiv(value.avgPaybackHitProb, value.count),
      avgConfidence: safeDiv(value.avgConfidence, value.count),
      avgPaybackMonths: value.paybackKnown > 0 ? safeDiv(value.avgPaybackMonths, value.paybackKnown) : null,
      paybackKnownCount: value.paybackKnown,
    }))
    .sort((a, b) => b.count - a.count);

  const scorecard = {
    generatedAt: new Date().toISOString(),
    modelVersion: MODEL_VERSION,
    calibrationVersion: CALIBRATION_VERSION,
    counts: {
      rows: rows.length,
      structuralRows: structuralRows.length,
      usableConsumptionRows: consumptionRows.length,
    },
    brier: {
      tight: brierScore(tightItems),
      priceUp: brierScore(priceUpItems),
      consumption: consumptionByHorizon,
    },
    representativeCohorts,
    inferabilityDistribution: {
      totalCohorts: latestTrendByCohort.size,
      bins: inferabilityBins,
      byRegime: Object.fromEntries([...inferabilityByRegime.entries()]),
    },
    recommendationOutcomeSummary,
  };

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(SCORECARD_JSON, JSON.stringify(scorecard, null, 2));
  await writeFile(COHORT_COMPARE_JSON, JSON.stringify(representativeCohorts, null, 2));
  await writeFile(INFERABILITY_JSON, JSON.stringify(scorecard.inferabilityDistribution, null, 2));

  const md = [
    "# Validation Scorecard v3.2",
    "",
    `Generated: ${scorecard.generatedAt}`,
    `Model: ${MODEL_VERSION}`,
    `Calibration: ${CALIBRATION_VERSION}`,
    "",
    "## Core Brier",
    `- Tight: ${scorecard.brier.tight.toFixed(4)}`,
    `- Price-up: ${scorecard.brier.priceUp.toFixed(4)}`,
    ...scorecard.brier.consumption.map(
      (row) =>
        `- Consumption ${row.horizonHours}h: raw=${row.brierRaw?.toFixed(4) ?? "n/a"}, calibrated=${row.brierCalibrated?.toFixed(4) ?? "n/a"}, legacy-proxy=${row.brierLegacyProxy?.toFixed(4) ?? "n/a"}, usable=${row.usable}, censored=${row.censored}`,
    ),
    "",
    "## Inferability Distribution",
    `- Total cohorts: ${scorecard.inferabilityDistribution.totalCohorts}`,
    `- Exactly 0: ${scorecard.inferabilityDistribution.bins["0"]}`,
    `- 0-10: ${scorecard.inferabilityDistribution.bins["0-10"]}`,
    `- 10-25: ${scorecard.inferabilityDistribution.bins["10-25"]}`,
    `- 25-50: ${scorecard.inferabilityDistribution.bins["25-50"]}`,
    `- 50-75: ${scorecard.inferabilityDistribution.bins["50-75"]}`,
    `- 75-100: ${scorecard.inferabilityDistribution.bins["75-100"]}`,
    "",
    "## Representative Cohorts",
    ...representativeCohorts.flatMap((entry) => [
      `- ${entry.role}: ${entry.cohort.key}`,
      `  state=${entry.cohort.state}, confidence=${entry.cohort.confidence.toFixed(1)}, inferability=${entry.cohort.inferability.toFixed(1)}, timeDepth=${entry.cohort.timeDepthScore.toFixed(1)}, crossSectionDepth=${entry.cohort.crossSectionDepthScore.toFixed(1)}`,
      `  usable/censored 12h=${entry.cohort.usage.usable12}/${entry.cohort.usage.censored12}, 24h=${entry.cohort.usage.usable24}/${entry.cohort.usage.censored24}, 72h=${entry.cohort.usage.usable72}/${entry.cohort.usage.censored72}`,
    ]),
    "",
    "## Recommendation Outcomes (Scenario Forecasts)",
    ...recommendationOutcomeSummary.map(
      (row) =>
        `- ${row.recommendation}: n=${row.count}, avgUtil=${(row.avgExpectedUtilization * 100).toFixed(1)}%, avgHit=${(row.avgPaybackHitProb * 100).toFixed(1)}%, avgConfidence=${row.avgConfidence.toFixed(1)}, avgPaybackMonths=${row.avgPaybackMonths == null ? "n/a" : row.avgPaybackMonths.toFixed(2)}`,
    ),
    "",
  ].join("\n");
  await writeFile(SCORECARD_MD, md);

  console.log("=== Backtest Summary ===");
  console.log(`Rows: ${rows.length}`);
  console.log(`Structural rows (tight/price): ${structuralRows.length}`);
  console.log(`Brier (tight): ${scorecard.brier.tight.toFixed(4)}`);
  console.log(`Brier (price up): ${scorecard.brier.priceUp.toFixed(4)}`);
  for (const row of scorecard.brier.consumption) {
    console.log(
      `Brier (consumption ${row.horizonHours}h): raw=${row.brierRaw?.toFixed(4) ?? "n/a"} calibrated=${row.brierCalibrated?.toFixed(4) ?? "n/a"} legacy=${row.brierLegacyProxy?.toFixed(4) ?? "n/a"} usable=${row.usable} censored=${row.censored}`,
    );
  }

  console.log("");
  console.log("Wrote artifacts:");
  console.log(`- ${SCORECARD_JSON}`);
  console.log(`- ${SCORECARD_MD}`);
  console.log(`- ${COHORT_COMPARE_JSON}`);
  console.log(`- ${INFERABILITY_JSON}`);
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

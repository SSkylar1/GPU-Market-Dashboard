import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { scoreScenarioWithMarket } from "@/lib/scoring/marketScore";
import { estimatePowerAndCost } from "@/lib/scoring/hardwareDefaults";

const inputSchema = z.object({
  gpuName: z.string().min(1),
  cohortNumGpus: z.number().int().positive().optional(),
  cohortOfferType: z.string().optional(),
  gpuCount: z.number().int().positive().max(128),
  assumedPowerWatts: z.number().int().positive().max(200000),
  assumedHardwareCost: z.number().positive(),
  electricityCostPerKwh: z.number().min(0).max(5),
  targetPaybackMonths: z.number().int().positive().max(120),
  source: z.string().optional(),
  hoursWindow: z.number().int().min(6).max(24 * 21).optional(),
  listingPricePerHour: z.number().positive().optional(),
});

export async function POST(request: Request) {
  const payload = await request.json();
  const parsed = inputSchema.safeParse(payload);

  if (!parsed.success) {
    return Response.json(
      { error: "Invalid scoring payload", issues: parsed.error.issues },
      { status: 400 },
    );
  }

  try {
    const result = await scoreScenarioWithMarket(parsed.data);

    return Response.json({
      ...result,
      overallScore: result.legacy.overallScore,
      demandScore: result.legacy.demandScore,
      competitionScore: result.legacy.competitionScore,
      priceStrengthScore: result.legacy.priceStrengthScore,
      efficiencyScore: result.legacy.efficiencyScore,
      expectedDailyRevenue: result.economics.expectedDailyRevenue,
      expectedDailyPowerCost:
        result.economics.expectedDailyRevenue - result.economics.expectedDailyMargin,
      expectedDailyProfit: result.economics.expectedDailyMargin,
      expectedPaybackMonths: result.economics.expectedPaybackMonths,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown scoring error";
    return Response.json({ error: message }, { status: 422 });
  }
}

export async function GET() {
  const source = "vast-live";

  const cohorts = await prisma.gpuTrendAggregate.findMany({
    where: { source },
    distinct: ["gpuName", "numGpus", "offerType"],
    select: { gpuName: true, numGpus: true, offerType: true },
    orderBy: [{ gpuName: "asc" }, { numGpus: "asc" }, { offerType: "asc" }],
  });

  const latestByCohort = new Map<string, {
    medianPrice: number | null;
    impliedUtilization: number;
    bucketStartUtc: Date;
    cohortPressureScore: number | null;
    state: string | null;
    stateConfidence: number | null;
    uniqueMachines: number | null;
    inferabilityScore: number | null;
    signalStrengthScore: number | null;
  }>();

  const latestRows = await prisma.gpuTrendAggregate.findMany({
    where: { source },
    orderBy: { bucketStartUtc: "desc" },
    take: 1000,
    select: {
      gpuName: true,
      numGpus: true,
      offerType: true,
      medianPrice: true,
      impliedUtilization: true,
      bucketStartUtc: true,
      cohortPressureScore: true,
      state: true,
      stateConfidence: true,
      uniqueMachines: true,
      inferabilityScore: true,
      signalStrengthScore: true,
    },
  });

  for (const row of latestRows) {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
    if (!latestByCohort.has(key)) {
      latestByCohort.set(key, row);
    }
  }

  const cohortTypeByGpuAndCount = new Map<string, Set<string>>();
  for (const row of cohorts) {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}`;
    const type = (row.offerType ?? "unknown").trim().toLowerCase() || "unknown";
    const current = cohortTypeByGpuAndCount.get(key) ?? new Set<string>();
    current.add(type);
    cohortTypeByGpuAndCount.set(key, current);
  }

  const filtered = cohorts.filter((row) => {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}`;
    const type = (row.offerType ?? "unknown").trim().toLowerCase() || "unknown";
    const types = cohortTypeByGpuAndCount.get(key);
    if (!types) return true;
    return type !== "unknown" || types.size === 1;
  });

  const gpuOptions = filtered.map((row) => {
    const key = `${row.gpuName}::${row.numGpus ?? "combined"}::${row.offerType ?? "combined"}`;
    const latest = latestByCohort.get(key);

    return {
      gpuName: row.gpuName,
      cohortNumGpus: row.numGpus,
      cohortOfferType: row.offerType,
      source,
      latestMedianPrice: latest?.medianPrice ?? null,
      latestImpliedUtilization: latest?.impliedUtilization ?? null,
      latestBucketUtc: latest?.bucketStartUtc.toISOString() ?? null,
      latestPressure: latest?.cohortPressureScore ?? null,
      latestState: latest?.state ?? null,
      latestStateConfidence: latest?.stateConfidence ?? null,
      latestUniqueMachines: latest?.uniqueMachines ?? null,
      latestInferabilityScore: latest?.inferabilityScore ?? null,
      latestSignalStrengthScore: latest?.signalStrengthScore ?? null,
      defaults: {
        ...estimatePowerAndCost(row.gpuName),
        gpuCount: row.numGpus ?? 1,
        electricityCostPerKwh: 0.12,
        targetPaybackMonths: 18,
        hoursWindow: 24 * 7,
      },
    };
  });

  const recentScenarioForecasts = await prisma.scenarioForecast.findMany({
    orderBy: { createdAt: "desc" },
    take: 30,
    include: {
      scenario: {
        select: {
          id: true,
          gpuName: true,
          gpuCount: true,
          targetPaybackMonths: true,
        },
      },
    },
  });

  return Response.json({
    gpuOptions,
    recentScenarios: recentScenarioForecasts.map((item) => ({
      id: item.scenario.id,
      gpuName: item.scenario.gpuName,
      gpuCount: item.scenario.gpuCount,
      createdAt: item.createdAt,
      targetPaybackMonths: item.scenario.targetPaybackMonths,
      latestScore: {
        overallScore: item.expectedPaybackMonths == null ? 0 : Number((100 / Math.max(item.expectedPaybackMonths, 1)).toFixed(2)),
        recommendation: item.recommendation,
        createdAt: item.createdAt,
      },
      confidenceScore: item.confidenceScore,
      expectedUtilization: item.expectedUtilization,
      expectedPaybackMonths: item.expectedPaybackMonths,
    })),
  });
}

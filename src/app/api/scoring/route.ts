import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { scoreScenarioWithMarket } from "@/lib/scoring/marketScore";
import { estimatePowerAndCost } from "@/lib/scoring/hardwareDefaults";

const inputSchema = z.object({
  gpuName: z.string().min(1),
  cohortNumGpus: z.number().int().positive().optional(),
  cohortOfferType: z.string().optional(),
  gpuCount: z.number().int().positive(),
  assumedPowerWatts: z.number().int().positive(),
  assumedHardwareCost: z.number().positive(),
  electricityCostPerKwh: z.number().min(0),
  targetPaybackMonths: z.number().int().positive(),
  source: z.string().optional(),
  hoursWindow: z.number().int().positive().optional(),
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
    return Response.json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown scoring error";
    return Response.json({ error: message }, { status: 422 });
  }
}

export async function GET() {
  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: { source: string };
        distinct: ["gpuName", "numGpus", "offerType"];
        select: { gpuName: true; numGpus: true; offerType: true };
        orderBy: [{ gpuName: "asc" }, { numGpus: "asc" }, { offerType: "asc" }];
      }) => Promise<Array<{ gpuName: string; numGpus: number; offerType: string }>>;
      findFirst: (args: {
        where: { source: string; gpuName: string; numGpus: number; offerType: string };
        orderBy: { bucketStartUtc: "desc" };
        select: { medianPrice: true; impliedUtilization: true; bucketStartUtc: true };
      }) => Promise<{
        medianPrice: number | null;
        impliedUtilization: number;
        bucketStartUtc: Date;
      } | null>;
    };
  }).gpuTrendAggregate;

  if (!trendClient) {
    return Response.json({ gpuOptions: [], recentScenarios: [] });
  }

  const source = "vast-live";
  const gpuRows = await trendClient.findMany({
    where: { source },
    distinct: ["gpuName", "numGpus", "offerType"],
    select: { gpuName: true, numGpus: true, offerType: true },
    orderBy: [{ gpuName: "asc" }, { numGpus: "asc" }, { offerType: "asc" }],
  });

  const gpuOptions = await Promise.all(
    gpuRows.map(async (row) => {
      const latestPoint = await trendClient.findFirst({
        where: {
          source,
          gpuName: row.gpuName,
          numGpus: row.numGpus,
          offerType: row.offerType,
        },
        orderBy: { bucketStartUtc: "desc" },
        select: {
          medianPrice: true,
          impliedUtilization: true,
          bucketStartUtc: true,
        },
      });

      return {
        gpuName: row.gpuName,
        cohortNumGpus: row.numGpus,
        cohortOfferType: row.offerType,
        source,
        latestMedianPrice: latestPoint?.medianPrice ?? null,
        latestImpliedUtilization: latestPoint?.impliedUtilization ?? null,
        latestBucketUtc: latestPoint?.bucketStartUtc ?? null,
        defaults: {
          ...estimatePowerAndCost(row.gpuName),
          gpuCount: 1,
          electricityCostPerKwh: 0.12,
          targetPaybackMonths: 18,
          hoursWindow: 24,
        },
      };
    }),
  );

  const recentScenarios = await prisma.hardwareScenario.findMany({
    orderBy: { createdAt: "desc" },
    take: 12,
    include: {
      scores: {
        orderBy: { createdAt: "desc" },
        take: 1,
      },
    },
  });

  return Response.json({
    gpuOptions,
    recentScenarios: recentScenarios.map((scenario) => ({
      id: scenario.id,
      gpuName: scenario.gpuName,
      gpuCount: scenario.gpuCount,
      createdAt: scenario.createdAt,
      targetPaybackMonths: scenario.targetPaybackMonths,
      latestScore: scenario.scores[0]
        ? {
            overallScore: scenario.scores[0].overallScore,
            recommendation: scenario.scores[0].recommendation,
            createdAt: scenario.scores[0].createdAt,
          }
        : null,
    })),
  });
}

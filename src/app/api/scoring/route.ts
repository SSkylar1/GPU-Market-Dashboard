import { z } from "zod";
import { prisma } from "@/lib/db/prisma";
import { scoreScenarioWithMarket } from "@/lib/scoring/marketScore";

const inputSchema = z.object({
  gpuName: z.string().min(1),
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

function estimatePowerAndCost(gpuName: string): { assumedPowerWatts: number; assumedHardwareCost: number } {
  const upper = gpuName.toUpperCase();

  if (upper.includes("H200")) return { assumedPowerWatts: 700, assumedHardwareCost: 30000 };
  if (upper.includes("H100")) return { assumedPowerWatts: 700, assumedHardwareCost: 25000 };
  if (upper.includes("A100")) return { assumedPowerWatts: 400, assumedHardwareCost: 12000 };
  if (upper.includes("A40")) return { assumedPowerWatts: 300, assumedHardwareCost: 4500 };
  if (upper.includes("L4")) return { assumedPowerWatts: 80, assumedHardwareCost: 2200 };
  if (upper.includes("5090")) return { assumedPowerWatts: 575, assumedHardwareCost: 2600 };
  if (upper.includes("5080")) return { assumedPowerWatts: 420, assumedHardwareCost: 1800 };
  if (upper.includes("5070")) return { assumedPowerWatts: 300, assumedHardwareCost: 1200 };
  if (upper.includes("5060")) return { assumedPowerWatts: 220, assumedHardwareCost: 800 };
  if (upper.includes("4090")) return { assumedPowerWatts: 450, assumedHardwareCost: 2500 };
  if (upper.includes("4080")) return { assumedPowerWatts: 320, assumedHardwareCost: 1400 };
  if (upper.includes("4070")) return { assumedPowerWatts: 285, assumedHardwareCost: 900 };
  if (upper.includes("3090")) return { assumedPowerWatts: 350, assumedHardwareCost: 700 };
  return { assumedPowerWatts: 350, assumedHardwareCost: 1500 };
}

export async function GET() {
  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: { source: string };
        distinct: ["gpuName"];
        select: { gpuName: true };
        orderBy: { gpuName: "asc" };
      }) => Promise<Array<{ gpuName: string }>>;
      findFirst: (args: {
        where: { source: string; gpuName: string };
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
    distinct: ["gpuName"],
    select: { gpuName: true },
    orderBy: { gpuName: "asc" },
  });

  const gpuOptions = await Promise.all(
    gpuRows.map(async (row) => {
      const latestPoint = await trendClient.findFirst({
        where: {
          source,
          gpuName: row.gpuName,
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

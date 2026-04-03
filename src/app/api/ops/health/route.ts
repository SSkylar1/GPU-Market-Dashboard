import { subHours } from "date-fns";
import { prisma } from "@/lib/db/prisma";

type HealthLevel = "healthy" | "warning" | "critical";

function classifyLagMinutes(lagMinutes: number, expectedCadenceMinutes: number): HealthLevel {
  if (lagMinutes <= expectedCadenceMinutes * 2) return "healthy";
  if (lagMinutes <= expectedCadenceMinutes * 4) return "warning";
  return "critical";
}

function classifyAggregateLagMinutes(lagMinutes: number, expectedCadenceMinutes: number): HealthLevel {
  if (lagMinutes <= expectedCadenceMinutes * 3) return "healthy";
  if (lagMinutes <= expectedCadenceMinutes * 6) return "warning";
  return "critical";
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const sourceScope = url.searchParams.get("source")?.trim() || "vast-live";
  const expectedCadenceMinutes = Number(process.env.OPS_EXPECTED_POLLING_MINUTES ?? 5);
  const now = new Date();
  const since6h = subHours(now, 6);
  const since24h = subHours(now, 24);

  const [latestSnapshot, latestAggregate, snapshots6h, snapshots24h, latestScenarioForecast] = await Promise.all([
    prisma.marketSnapshot.findFirst({
      where: { source: sourceScope },
      orderBy: { capturedAt: "desc" },
      select: { id: true, source: true, ingestMode: true, capturedAt: true, createdAt: true },
    }),
    prisma.gpuTrendAggregate.findFirst({
      where: { source: sourceScope },
      orderBy: { bucketStartUtc: "desc" },
      select: { bucketStartUtc: true, updatedAt: true },
    }),
    prisma.marketSnapshot.findMany({
      where: { source: sourceScope, capturedAt: { gte: since6h } },
      orderBy: { capturedAt: "desc" },
      select: { id: true, source: true, ingestMode: true, capturedAt: true },
      take: 5000,
    }),
    prisma.marketSnapshot.findMany({
      where: { source: sourceScope, capturedAt: { gte: since24h } },
      orderBy: { capturedAt: "desc" },
      select: { id: true, source: true, capturedAt: true },
      take: 15000,
    }),
    prisma.scenarioForecast.findFirst({
      where: { source: sourceScope },
      orderBy: { createdAt: "desc" },
      select: { id: true, createdAt: true, source: true, modelVersion: true },
    }),
  ]);
  const [globalSnapshots6h, globalSnapshots24h] = await Promise.all([
    prisma.marketSnapshot.findMany({
      where: { capturedAt: { gte: since6h } },
      orderBy: { capturedAt: "desc" },
      select: { source: true },
      take: 5000,
    }),
    prisma.marketSnapshot.findMany({
      where: { capturedAt: { gte: since24h } },
      orderBy: { capturedAt: "desc" },
      select: { source: true },
      take: 15000,
    }),
  ]);

  const snapshotLagMinutes =
    latestSnapshot == null ? null : Math.max(0, Math.floor((now.getTime() - latestSnapshot.capturedAt.getTime()) / 60000));
  const aggregateBucketLagMinutes =
    latestAggregate == null ? null : Math.max(0, Math.floor((now.getTime() - latestAggregate.bucketStartUtc.getTime()) / 60000));
  const aggregateUpdateLagMinutes =
    latestAggregate == null ? null : Math.max(0, Math.floor((now.getTime() - latestAggregate.updatedAt.getTime()) / 60000));

  const snapshotHealth =
    snapshotLagMinutes == null ? "critical" : classifyLagMinutes(snapshotLagMinutes, expectedCadenceMinutes);
  const aggregateHealth =
    aggregateUpdateLagMinutes == null
      ? "critical"
      : classifyAggregateLagMinutes(aggregateUpdateLagMinutes, expectedCadenceMinutes);

  const snapshots6hSorted = [...snapshots6h].sort((a, b) => a.capturedAt.getTime() - b.capturedAt.getTime());
  const pollGaps: number[] = [];
  for (let i = 1; i < snapshots6hSorted.length; i += 1) {
    pollGaps.push((snapshots6hSorted[i].capturedAt.getTime() - snapshots6hSorted[i - 1].capturedAt.getTime()) / 60000);
  }

  const avgGapMinutes = pollGaps.length === 0 ? null : pollGaps.reduce((sum, value) => sum + value, 0) / pollGaps.length;
  const maxGapMinutes = pollGaps.length === 0 ? null : Math.max(...pollGaps);
  const cadenceHealth =
    avgGapMinutes == null
      ? "warning"
      : avgGapMinutes <= expectedCadenceMinutes * 1.6
        ? "healthy"
        : avgGapMinutes <= expectedCadenceMinutes * 3
          ? "warning"
          : "critical";

  const snapshotsBySource6h = globalSnapshots6h.reduce<Record<string, number>>((acc, row) => {
    acc[row.source] = (acc[row.source] ?? 0) + 1;
    return acc;
  }, {});
  const snapshotsBySource24h = globalSnapshots24h.reduce<Record<string, number>>((acc, row) => {
    acc[row.source] = (acc[row.source] ?? 0) + 1;
    return acc;
  }, {});

  const overallHealth: HealthLevel =
    snapshotHealth === "critical" || aggregateHealth === "critical" || cadenceHealth === "critical"
      ? "critical"
      : snapshotHealth === "warning" || aggregateHealth === "warning" || cadenceHealth === "warning"
        ? "warning"
        : "healthy";

  return Response.json({
    nowUtc: now.toISOString(),
    sourceScope,
    overallHealth,
    expectedCadenceMinutes,
    latestSnapshot: latestSnapshot
      ? {
          ...latestSnapshot,
          lagMinutes: snapshotLagMinutes,
          health: snapshotHealth,
        }
      : null,
    latestAggregate: latestAggregate
      ? {
          ...latestAggregate,
          bucketLagMinutes: aggregateBucketLagMinutes,
          lagMinutes: aggregateUpdateLagMinutes,
          health: aggregateHealth,
        }
      : null,
    cadence: {
      health: cadenceHealth,
      snapshots6h: snapshots6h.length,
      snapshots24h: snapshots24h.length,
      avgGapMinutes: avgGapMinutes == null ? null : Number(avgGapMinutes.toFixed(2)),
      maxGapMinutes: maxGapMinutes == null ? null : Number(maxGapMinutes.toFixed(2)),
    },
    sourceMix: {
      snapshotsBySource6h,
      snapshotsBySource24h,
    },
    latestScenarioForecast,
  });
}

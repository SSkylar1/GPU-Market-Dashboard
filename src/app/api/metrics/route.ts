import { prisma } from "@/lib/db/prisma";

export async function GET() {
  const trendClient = (prisma as unknown as { gpuTrendAggregate?: {
    findFirst: (args: { orderBy: { bucketStartUtc: "desc" }; select: { bucketStartUtc: true } }) => Promise<{ bucketStartUtc: Date } | null>;
  } }).gpuTrendAggregate;

  const [snapshots, latestAggregateBucket] = await Promise.all([
    prisma.marketSnapshot.findMany({
      orderBy: { capturedAt: "desc" },
      take: 2,
      include: {
        rollups: {
          orderBy: { totalOffers: "desc" },
          take: 50,
        },
      },
    }),
    trendClient
      ? trendClient.findFirst({
          orderBy: { bucketStartUtc: "desc" },
          select: { bucketStartUtc: true },
        })
      : Promise.resolve(null),
  ]);

  const latestSnapshot = snapshots[0] ?? null;
  const previousSnapshot = snapshots[1] ?? null;
  const previousMap = new Map(
    (previousSnapshot?.rollups ?? []).map((rollup) => [rollup.gpuName, rollup]),
  );

  const rollupsWithDelta = (latestSnapshot?.rollups ?? []).map((rollup) => {
    const previous = previousMap.get(rollup.gpuName);
    return {
      ...rollup,
      medianPriceDelta:
        previous?.medianPrice != null && rollup.medianPrice != null
          ? rollup.medianPrice - previous.medianPrice
          : null,
      impliedUtilizationDelta:
        previous != null ? rollup.impliedUtilization - previous.impliedUtilization : null,
    };
  });

  const freshnessMinutes =
    latestSnapshot == null ? null : Math.max(0, Math.floor((Date.now() - latestSnapshot.capturedAt.getTime()) / 60000));

  return Response.json({
    snapshotId: latestSnapshot?.id ?? null,
    capturedAt: latestSnapshot?.capturedAt ?? null,
    previousSnapshotId: previousSnapshot?.id ?? null,
    latestAggregateBucketUtc: latestAggregateBucket?.bucketStartUtc ?? null,
    freshnessMinutes,
    rollups: rollupsWithDelta,
  });
}

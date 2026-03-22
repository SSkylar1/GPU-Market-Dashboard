import { prisma } from "@/lib/db/prisma";
import { subHours } from "date-fns";

export async function GET() {
  const trendClient = (prisma as unknown as { gpuTrendAggregate?: {
    findFirst: (args: { orderBy: { bucketStartUtc: "desc" }; select: { bucketStartUtc: true } }) => Promise<{ bucketStartUtc: Date } | null>;
    findMany: (args: {
      where: {
        source: string;
        bucketStartUtc: { gte: Date };
      };
      select: {
        gpuName: true;
        totalOffers: true;
        rentableOffers: true;
        rentedOffers: true;
        medianPrice: true;
      };
    }) => Promise<Array<{
      gpuName: string;
      totalOffers: number;
      rentableOffers: number;
      rentedOffers: number;
      medianPrice: number | null;
    }>>;
  } }).gpuTrendAggregate;

  const [latestSnapshot, latestAggregateBucket] = await Promise.all([
    prisma.marketSnapshot.findFirst({
      orderBy: { capturedAt: "desc" },
      include: {
        offers: {
          select: {
            gpuName: true,
            hostId: true,
            machineId: true,
          },
        },
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

  const previousSnapshot =
    latestSnapshot?.sourceQueryHash == null
      ? null
      : await prisma.marketSnapshot.findFirst({
          where: {
            sourceQueryHash: latestSnapshot.sourceQueryHash,
            id: {
              not: latestSnapshot.id,
            },
          },
          orderBy: { capturedAt: "desc" },
          include: {
            rollups: true,
          },
        });
  const previousMap = new Map(
    (previousSnapshot?.rollups ?? []).map((rollup) => [rollup.gpuName, rollup]),
  );
  const supplyMap = new Map<
    string,
    { hostIds: Set<number>; machineIds: Set<number> }
  >();
  for (const offer of latestSnapshot?.offers ?? []) {
    const current = supplyMap.get(offer.gpuName) ?? {
      hostIds: new Set<number>(),
      machineIds: new Set<number>(),
    };
    if (offer.hostId != null) {
      current.hostIds.add(offer.hostId);
    }
    if (offer.machineId != null) {
      current.machineIds.add(offer.machineId);
    }
    supplyMap.set(offer.gpuName, current);
  }

  const windowStart = latestSnapshot ? subHours(latestSnapshot.capturedAt, 24) : subHours(new Date(), 24);
  const trendRows =
    trendClient && latestSnapshot
      ? await trendClient.findMany({
          where: {
            source: latestSnapshot.source,
            bucketStartUtc: { gte: windowStart },
          },
          select: {
            gpuName: true,
            totalOffers: true,
            rentableOffers: true,
            rentedOffers: true,
            medianPrice: true,
          },
        })
      : [];
  const trendStatsMap = new Map<
    string,
    {
      bucketCount: number;
      avgTotalOffers: number;
      avgRentableOffers: number;
      weightedImpliedUtilization: number;
      weightedObservedRentedShare: number;
      medianOfBucketMedians: number | null;
    }
  >();
  const groupedTrendRows = new Map<string, typeof trendRows>();
  for (const row of trendRows) {
    const current = groupedTrendRows.get(row.gpuName) ?? [];
    current.push(row);
    groupedTrendRows.set(row.gpuName, current);
  }
  for (const [gpuName, rows] of groupedTrendRows.entries()) {
    const bucketCount = rows.length;
    const sumTotal = rows.reduce((acc, row) => acc + row.totalOffers, 0);
    const sumRentable = rows.reduce((acc, row) => acc + row.rentableOffers, 0);
    const sumRented = rows.reduce((acc, row) => acc + row.rentedOffers, 0);
    const medians = rows
      .map((row) => row.medianPrice)
      .filter((value): value is number => value != null)
      .sort((a, b) => a - b);
    const medianOfBucketMedians =
      medians.length === 0 ? null : medians[Math.ceil(medians.length * 0.5) - 1];

    trendStatsMap.set(gpuName, {
      bucketCount,
      avgTotalOffers: bucketCount === 0 ? 0 : sumTotal / bucketCount,
      avgRentableOffers: bucketCount === 0 ? 0 : sumRentable / bucketCount,
      weightedImpliedUtilization: sumTotal === 0 ? 0 : 1 - sumRentable / sumTotal,
      weightedObservedRentedShare: sumTotal === 0 ? 0 : sumRented / sumTotal,
      medianOfBucketMedians,
    });
  }

  const rollupsWithDelta = (latestSnapshot?.rollups ?? []).map((rollup) => {
    const previous = previousMap.get(rollup.gpuName);
    const supply = supplyMap.get(rollup.gpuName);
    const trendStats = trendStatsMap.get(rollup.gpuName);
    const observedRentedShare =
      rollup.totalOffers === 0 ? 0 : rollup.rentedOffers / rollup.totalOffers;
    const previousObservedRentedShare =
      previous == null || previous.totalOffers === 0 ? null : previous.rentedOffers / previous.totalOffers;
    return {
      ...rollup,
      observedRentedShare,
      window24h: trendStats ?? null,
      distinctHostCount: supply?.hostIds.size ?? 0,
      distinctMachineCount: supply?.machineIds.size ?? 0,
      medianPriceDelta:
        previous?.medianPrice != null && rollup.medianPrice != null
          ? rollup.medianPrice - previous.medianPrice
          : null,
      impliedUtilizationDelta:
        previous != null ? rollup.impliedUtilization - previous.impliedUtilization : null,
      observedRentedShareDelta:
        previousObservedRentedShare == null ? null : observedRentedShare - previousObservedRentedShare,
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

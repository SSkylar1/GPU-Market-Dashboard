import { prisma } from "@/lib/db/prisma";
import { subDays, subHours } from "date-fns";

type TrendRow = {
  gpuName: string;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  medianPrice: number | null;
};

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function clamp(value: number, min = 0, max = 100): number {
  return Math.max(min, Math.min(max, value));
}

function slope(xs: number[], ys: number[]): number | null {
  if (xs.length < 3 || ys.length < 3 || xs.length !== ys.length) return null;
  const xMean = mean(xs);
  const yMean = mean(ys);
  let num = 0;
  let den = 0;
  for (let i = 0; i < xs.length; i += 1) {
    const dx = xs[i] - xMean;
    const dy = ys[i] - yMean;
    num += dx * dy;
    den += dx * dx;
  }
  if (den === 0) return null;
  return num / den;
}

function computeWindowStats(rows: TrendRow[], hasSecondaryLeaseSignal: boolean) {
  const bucketCount = rows.length;
  if (bucketCount === 0) {
    return {
      bucketCount: 0,
      avgTotalOffers: 0,
      avgAvailableOffers: 0,
      unavailableShare: 0,
      activeLeaseShare: 0,
      medianOfBucketMedians: null as number | null,
      availabilityElasticityPerDollar: null as number | null,
      confidenceScore: 0,
    };
  }

  const totalOffers = rows.reduce((acc, row) => acc + row.totalOffers, 0);
  const availableOffers = rows.reduce((acc, row) => acc + row.rentableOffers, 0);
  const activeLeaseOffers = rows.reduce((acc, row) => acc + row.rentedOffers, 0);

  const medianPrices = rows
    .map((row) => row.medianPrice)
    .filter((value): value is number => value != null)
    .sort((a, b) => a - b);

  const medianOfBucketMedians =
    medianPrices.length === 0
      ? null
      : medianPrices[Math.ceil(medianPrices.length * 0.5) - 1];

  const elasticityRows = rows
    .filter((row) => row.totalOffers > 0 && row.medianPrice != null)
    .map((row) => ({
      price: row.medianPrice as number,
      availableShare: row.rentableOffers / row.totalOffers,
    }));

  const elasticity = slope(
    elasticityRows.map((row) => row.price),
    elasticityRows.map((row) => row.availableShare),
  );

  const unavailableShare = totalOffers === 0 ? 0 : 1 - availableOffers / totalOffers;
  const activeLeaseShare = totalOffers === 0 ? 0 : activeLeaseOffers / totalOffers;
  const availableShare = totalOffers === 0 ? 0 : availableOffers / totalOffers;
  const coverageShare = Math.min(1, availableShare + activeLeaseShare);

  const bucketScore = clamp((bucketCount / 48) * 100);
  const coverageScore = clamp(coverageShare * 100);
  const leaseSignalScore = hasSecondaryLeaseSignal ? 100 : 35;
  const confidenceScore = clamp(bucketScore * 0.45 + coverageScore * 0.35 + leaseSignalScore * 0.2);

  return {
    bucketCount,
    avgTotalOffers: totalOffers / bucketCount,
    avgAvailableOffers: availableOffers / bucketCount,
    unavailableShare,
    activeLeaseShare,
    medianOfBucketMedians,
    availabilityElasticityPerDollar: elasticity == null ? null : elasticity * 100,
    confidenceScore,
  };
}

export async function GET() {
  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findFirst: (args: {
        orderBy: { bucketStartUtc: "desc" };
        select: { bucketStartUtc: true };
      }) => Promise<{ bucketStartUtc: Date } | null>;
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
      }) => Promise<TrendRow[]>;
    };
  }).gpuTrendAggregate;

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

  const sourceQueryRecord =
    latestSnapshot?.sourceQuery && typeof latestSnapshot.sourceQuery === "object"
      ? (latestSnapshot.sourceQuery as Record<string, unknown>)
      : null;
  const hasSecondaryLeaseSignal =
    sourceQueryRecord != null &&
    typeof sourceQueryRecord.activeLeasesEndpoint === "string" &&
    sourceQueryRecord.activeLeasesEndpoint.trim().length > 0;

  const supplyMap = new Map<string, { hostIds: Set<number>; machineIds: Set<number> }>();
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

  const start24h = latestSnapshot ? subHours(latestSnapshot.capturedAt, 24) : subHours(new Date(), 24);
  const start7d = latestSnapshot ? subDays(latestSnapshot.capturedAt, 7) : subDays(new Date(), 7);

  const [rows24h, rows7d] =
    trendClient && latestSnapshot
      ? await Promise.all([
          trendClient.findMany({
            where: {
              source: latestSnapshot.source,
              bucketStartUtc: { gte: start24h },
            },
            select: {
              gpuName: true,
              totalOffers: true,
              rentableOffers: true,
              rentedOffers: true,
              medianPrice: true,
            },
          }),
          trendClient.findMany({
            where: {
              source: latestSnapshot.source,
              bucketStartUtc: { gte: start7d },
            },
            select: {
              gpuName: true,
              totalOffers: true,
              rentableOffers: true,
              rentedOffers: true,
              medianPrice: true,
            },
          }),
        ])
      : [[], []];

  const grouped24h = new Map<string, TrendRow[]>();
  for (const row of rows24h) {
    const current = grouped24h.get(row.gpuName) ?? [];
    current.push(row);
    grouped24h.set(row.gpuName, current);
  }

  const grouped7d = new Map<string, TrendRow[]>();
  for (const row of rows7d) {
    const current = grouped7d.get(row.gpuName) ?? [];
    current.push(row);
    grouped7d.set(row.gpuName, current);
  }

  const stats24h = new Map<string, ReturnType<typeof computeWindowStats>>();
  for (const [gpuName, rows] of grouped24h.entries()) {
    stats24h.set(gpuName, computeWindowStats(rows, hasSecondaryLeaseSignal));
  }

  const stats7d = new Map<string, ReturnType<typeof computeWindowStats>>();
  for (const [gpuName, rows] of grouped7d.entries()) {
    stats7d.set(gpuName, computeWindowStats(rows, hasSecondaryLeaseSignal));
  }

  const rollupsWithDelta = (latestSnapshot?.rollups ?? []).map((rollup) => {
    const previous = previousMap.get(rollup.gpuName);
    const supply = supplyMap.get(rollup.gpuName);
    const availableShare =
      rollup.totalOffers === 0 ? 0 : rollup.rentableOffers / rollup.totalOffers;
    const leaseSignalShare =
      rollup.totalOffers === 0 ? 0 : rollup.rentedOffers / rollup.totalOffers;

    const window24hRaw = stats24h.get(rollup.gpuName) ?? null;
    const window7dRaw = stats7d.get(rollup.gpuName) ?? null;
    const window24h =
      window24hRaw == null
        ? null
        : {
            ...window24hRaw,
            leaseSignalShare: window24hRaw.activeLeaseShare,
          };
    const window7d =
      window7dRaw == null
        ? null
        : {
            ...window7dRaw,
            leaseSignalShare: window7dRaw.activeLeaseShare,
          };

    return {
      ...rollup,
      availableShare,
      unavailableShare: rollup.impliedUtilization,
      leaseSignalShare,
      leaseSignalQuality: hasSecondaryLeaseSignal ? "high" : "low",
      window24h,
      window7d,
      distinctHostCount: supply?.hostIds.size ?? 0,
      distinctMachineCount: supply?.machineIds.size ?? 0,
      medianPriceDelta:
        previous?.medianPrice != null && rollup.medianPrice != null
          ? rollup.medianPrice - previous.medianPrice
          : null,
      unavailableShareDelta:
        previous != null ? rollup.impliedUtilization - previous.impliedUtilization : null,
    };
  });

  const freshnessMinutes =
    latestSnapshot == null
      ? null
      : Math.max(0, Math.floor((Date.now() - latestSnapshot.capturedAt.getTime()) / 60000));

  return Response.json({
    snapshotId: latestSnapshot?.id ?? null,
    capturedAt: latestSnapshot?.capturedAt ?? null,
    previousSnapshotId: previousSnapshot?.id ?? null,
    latestAggregateBucketUtc: latestAggregateBucket?.bucketStartUtc ?? null,
    freshnessMinutes,
    leaseSignalQuality: hasSecondaryLeaseSignal ? "high" : "low",
    rollups: rollupsWithDelta,
  });
}

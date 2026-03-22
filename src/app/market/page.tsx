import Link from "next/link";
import { subDays, subHours } from "date-fns";
import { prisma } from "@/lib/db/prisma";

export const dynamic = "force-dynamic";

type TrendRow = {
  gpuName: string;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  medianPrice: number | null;
};

type WindowStats = {
  bucketCount: number;
  avgTotalOffers: number;
  avgAvailableOffers: number;
  unavailableShare: number;
  activeLeaseShare: number;
  medianOfBucketMedians: number | null;
  availabilityElasticityPerDollar: number | null;
  confidenceScore: number;
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

function computeWindowStats(rows: TrendRow[], hasSecondaryLeaseSignal: boolean): WindowStats {
  const bucketCount = rows.length;
  if (bucketCount === 0) {
    return {
      bucketCount: 0,
      avgTotalOffers: 0,
      avgAvailableOffers: 0,
      unavailableShare: 0,
      activeLeaseShare: 0,
      medianOfBucketMedians: null,
      availabilityElasticityPerDollar: null,
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
    availabilityElasticityPerDollar:
      elasticity == null ? null : elasticity * 100,
    confidenceScore,
  };
}

export default async function MarketPage() {
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
          take: 20,
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
    if (offer.hostId != null) current.hostIds.add(offer.hostId);
    if (offer.machineId != null) current.machineIds.add(offer.machineId);
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
    const list = grouped24h.get(row.gpuName) ?? [];
    list.push(row);
    grouped24h.set(row.gpuName, list);
  }

  const grouped7d = new Map<string, TrendRow[]>();
  for (const row of rows7d) {
    const list = grouped7d.get(row.gpuName) ?? [];
    list.push(row);
    grouped7d.set(row.gpuName, list);
  }

  const stats24h = new Map<string, WindowStats>();
  for (const [gpuName, rows] of grouped24h.entries()) {
    stats24h.set(gpuName, computeWindowStats(rows, hasSecondaryLeaseSignal));
  }

  const stats7d = new Map<string, WindowStats>();
  for (const [gpuName, rows] of grouped7d.entries()) {
    stats7d.set(gpuName, computeWindowStats(rows, hasSecondaryLeaseSignal));
  }

  const nowResult = await prisma.$queryRaw<{ now: Date }[]>`SELECT NOW() AS now`;
  const nowUtc = nowResult[0]?.now ?? latestSnapshot?.capturedAt ?? null;
  const freshnessMinutes =
    latestSnapshot == null || nowUtc == null
      ? null
      : Math.max(0, Math.floor((nowUtc.getTime() - latestSnapshot.capturedAt.getTime()) / 60000));

  return (
    <main className="mx-auto w-full max-w-7xl p-6 md:p-10">
      <header className="mb-6 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">GPU Market Dashboard</h1>
          <p className="text-sm text-zinc-600">
            Latest snapshot: {latestSnapshot ? latestSnapshot.capturedAt.toISOString() : "No snapshots yet"}
          </p>
          <p className="text-sm text-zinc-600">
            Latest trend bucket (UTC): {latestAggregateBucket ? latestAggregateBucket.bucketStartUtc.toISOString() : "No trend buckets yet"}
            {freshnessMinutes != null ? ` · Freshness ${freshnessMinutes}m` : ""}
          </p>
          <p className="text-sm text-zinc-600">
            Core signal = Available/Unavailable shares. Lease signal confidence: {hasSecondaryLeaseSignal ? "High (secondary endpoint configured)" : "Low (secondary endpoint not configured)"}.
          </p>
        </div>
        <nav className="flex gap-3 text-sm">
          <Link className="text-blue-700 underline" href="/scoring">
            Scoring
          </Link>
          <Link className="text-blue-700 underline" href="/pricing">
            Pricing
          </Link>
        </nav>
      </header>

      {!latestSnapshot || latestSnapshot.rollups.length === 0 ? (
        <section className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm">
          No rollup data found. Run `npm run collect` then `npm run recompute`.
        </section>
      ) : (
        <div className="overflow-x-auto rounded border border-zinc-200">
          <table className="min-w-full divide-y divide-zinc-200 text-sm">
            <thead className="bg-zinc-50 text-left text-zinc-700">
              <tr>
                <th className="px-4 py-3 font-medium">GPU</th>
                <th className="px-4 py-3 font-medium">24h Avg Total</th>
                <th className="px-4 py-3 font-medium">24h Avg Available</th>
                <th className="px-4 py-3 font-medium">24h Unavailable %</th>
                <th className="px-4 py-3 font-medium">7d Avg Available</th>
                <th className="px-4 py-3 font-medium">7d Unavailable %</th>
                <th className="px-4 py-3 font-medium">7d Elasticity (Avail pts/$)</th>
                <th className="px-4 py-3 font-medium">Signal Confidence</th>
                <th className="px-4 py-3 font-medium">Latest Total</th>
                <th className="px-4 py-3 font-medium">Latest Unavailable %</th>
                <th className="px-4 py-3 font-medium">Hosts</th>
                <th className="px-4 py-3 font-medium">Machines</th>
                <th className="px-4 py-3 font-medium">Median Δ</th>
                <th className="px-4 py-3 font-medium">Unavailable Δ</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {latestSnapshot.rollups.map((row) => {
                const window24h = stats24h.get(row.gpuName);
                const window7d = stats7d.get(row.gpuName);
                return (
                  <tr key={row.id}>
                    <td className="px-4 py-3">
                      <Link className="text-blue-700 underline" href={`/gpus/${encodeURIComponent(row.gpuName)}`}>
                        {row.gpuName}
                      </Link>
                    </td>
                    <td className="px-4 py-3">{window24h ? window24h.avgTotalOffers.toFixed(1) : "-"}</td>
                    <td className="px-4 py-3">{window24h ? window24h.avgAvailableOffers.toFixed(1) : "-"}</td>
                    <td className="px-4 py-3">
                      {window24h ? `${(window24h.unavailableShare * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td className="px-4 py-3">{window7d ? window7d.avgAvailableOffers.toFixed(1) : "-"}</td>
                    <td className="px-4 py-3">
                      {window7d ? `${(window7d.unavailableShare * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td className="px-4 py-3">
                      {window7d?.availabilityElasticityPerDollar == null
                        ? "-"
                        : `${window7d.availabilityElasticityPerDollar.toFixed(2)}`}
                    </td>
                    <td className="px-4 py-3">
                      {window7d ? `${window7d.confidenceScore.toFixed(0)}/100` : "-"}
                    </td>
                    <td className="px-4 py-3">{row.totalOffers}</td>
                    <td className="px-4 py-3">{(row.impliedUtilization * 100).toFixed(1)}%</td>
                    <td className="px-4 py-3">{supplyMap.get(row.gpuName)?.hostIds.size ?? 0}</td>
                    <td className="px-4 py-3">{supplyMap.get(row.gpuName)?.machineIds.size ?? 0}</td>
                    <td className="px-4 py-3">
                      {(() => {
                        const previous = previousMap.get(row.gpuName);
                        if (!previous || previous.medianPrice == null || row.medianPrice == null) return "-";
                        const delta = row.medianPrice - previous.medianPrice;
                        const sign = delta > 0 ? "+" : "";
                        return `${sign}$${delta.toFixed(3)}/hr`;
                      })()}
                    </td>
                    <td className="px-4 py-3">
                      {(() => {
                        const previous = previousMap.get(row.gpuName);
                        if (!previous) return "-";
                        const deltaPct = (row.impliedUtilization - previous.impliedUtilization) * 100;
                        const sign = deltaPct > 0 ? "+" : "";
                        return `${sign}${deltaPct.toFixed(1)}%`;
                      })()}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

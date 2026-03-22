import Link from "next/link";
import { subHours } from "date-fns";
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
  avgRentableOffers: number;
  weightedImpliedUtilization: number;
  weightedObservedRentedShare: number;
  medianOfBucketMedians: number | null;
};

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

  const statsByGpu = new Map<string, WindowStats>();
  const grouped = new Map<string, TrendRow[]>();
  for (const row of trendRows) {
    const list = grouped.get(row.gpuName) ?? [];
    list.push(row);
    grouped.set(row.gpuName, list);
  }

  for (const [gpuName, rows] of grouped.entries()) {
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

    statsByGpu.set(gpuName, {
      bucketCount,
      avgTotalOffers: bucketCount === 0 ? 0 : sumTotal / bucketCount,
      avgRentableOffers: bucketCount === 0 ? 0 : sumRentable / bucketCount,
      weightedImpliedUtilization: sumTotal === 0 ? 0 : 1 - sumRentable / sumTotal,
      weightedObservedRentedShare: sumTotal === 0 ? 0 : sumRented / sumTotal,
      medianOfBucketMedians,
    });
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
            24h aggregates are from UTC half-hour buckets (same source as latest snapshot).
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
                <th className="px-4 py-3 font-medium">24h Avg Rentable</th>
                <th className="px-4 py-3 font-medium">24h Util (Weighted)</th>
                <th className="px-4 py-3 font-medium">24h Rented Share</th>
                <th className="px-4 py-3 font-medium">24h Median Price</th>
                <th className="px-4 py-3 font-medium">Latest Total</th>
                <th className="px-4 py-3 font-medium">Latest Util</th>
                <th className="px-4 py-3 font-medium">Hosts</th>
                <th className="px-4 py-3 font-medium">Machines</th>
                <th className="px-4 py-3 font-medium">Median Δ</th>
                <th className="px-4 py-3 font-medium">Util Δ</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {latestSnapshot.rollups.map((row) => {
                const stats = statsByGpu.get(row.gpuName);
                return (
                  <tr key={row.id}>
                    <td className="px-4 py-3">
                      <Link className="text-blue-700 underline" href={`/gpus/${encodeURIComponent(row.gpuName)}`}>
                        {row.gpuName}
                      </Link>
                    </td>
                    <td className="px-4 py-3">{stats ? stats.avgTotalOffers.toFixed(1) : "-"}</td>
                    <td className="px-4 py-3">{stats ? stats.avgRentableOffers.toFixed(1) : "-"}</td>
                    <td className="px-4 py-3">
                      {stats ? `${(stats.weightedImpliedUtilization * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td className="px-4 py-3">
                      {stats ? `${(stats.weightedObservedRentedShare * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td className="px-4 py-3">
                      {stats?.medianOfBucketMedians == null ? "-" : `$${stats.medianOfBucketMedians.toFixed(3)}/hr`}
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

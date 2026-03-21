import Link from "next/link";
import { prisma } from "@/lib/db/prisma";

export const dynamic = "force-dynamic";

export default async function MarketPage() {
  const trendClient = (prisma as unknown as { gpuTrendAggregate?: {
    findFirst: (args: { orderBy: { bucketStartUtc: "desc" }; select: { bucketStartUtc: true } }) => Promise<{ bucketStartUtc: Date } | null>;
  } }).gpuTrendAggregate;

  const [latestSnapshot, latestAggregateBucket] = await Promise.all([
    prisma.marketSnapshot.findFirst({
      orderBy: { capturedAt: "desc" },
      include: {
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
  const nowResult = await prisma.$queryRaw<{ now: Date }[]>`SELECT NOW() AS now`;
  const nowUtc = nowResult[0]?.now ?? latestSnapshot?.capturedAt ?? null;
  const freshnessMinutes =
    latestSnapshot == null || nowUtc == null
      ? null
      : Math.max(0, Math.floor((nowUtc.getTime() - latestSnapshot.capturedAt.getTime()) / 60000));

  return (
    <main className="mx-auto w-full max-w-6xl p-6 md:p-10">
      <header className="mb-6 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">GPU Market Dashboard</h1>
          <p className="text-sm text-zinc-600">
            Latest snapshot:{" "}
            {latestSnapshot ? latestSnapshot.capturedAt.toISOString() : "No snapshots yet"}
          </p>
          <p className="text-sm text-zinc-600">
            Latest trend bucket (UTC):{" "}
            {latestAggregateBucket ? latestAggregateBucket.bucketStartUtc.toISOString() : "No trend buckets yet"}
            {freshnessMinutes != null ? ` · Freshness ${freshnessMinutes}m` : ""}
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
                <th className="px-4 py-3 font-medium">Total</th>
                <th className="px-4 py-3 font-medium">Rentable</th>
                <th className="px-4 py-3 font-medium">Implied Utilization</th>
                <th className="px-4 py-3 font-medium">Median Price</th>
                <th className="px-4 py-3 font-medium">Median Δ</th>
                <th className="px-4 py-3 font-medium">Util Δ</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {latestSnapshot.rollups.map((row) => (
                <tr key={row.id}>
                  <td className="px-4 py-3">
                    <Link
                      className="text-blue-700 underline"
                      href={`/gpus/${encodeURIComponent(row.gpuName)}`}
                    >
                      {row.gpuName}
                    </Link>
                  </td>
                  <td className="px-4 py-3">{row.totalOffers}</td>
                  <td className="px-4 py-3">{row.rentableOffers}</td>
                  <td className="px-4 py-3">
                    {(row.impliedUtilization * 100).toFixed(1)}%
                  </td>
                  <td className="px-4 py-3">
                    {row.medianPrice == null ? "-" : `$${row.medianPrice.toFixed(3)}/hr`}
                  </td>
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
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

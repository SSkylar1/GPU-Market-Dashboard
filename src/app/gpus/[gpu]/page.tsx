import Link from "next/link";
import { subHours } from "date-fns";
import { prisma } from "@/lib/db/prisma";

export const dynamic = "force-dynamic";

export default async function GpuDetailPage({
  params,
}: {
  params: Promise<{ gpu: string }>;
}) {
  const { gpu } = await params;
  const gpuName = decodeURIComponent(gpu);
  const since = subHours(new Date(), 24);

  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: { gpuName: string; bucketStartUtc: { gte: Date } };
        orderBy: { bucketStartUtc: "asc" };
        take: number;
      }) => Promise<Array<{
        id: string;
        source: string;
        bucketStartUtc: Date;
        snapshotCount: number;
        totalOffers: number;
        impliedUtilization: number;
        availabilityRatio: number;
        medianPrice: number | null;
        p90Price: number | null;
      }>>;
    };
  }).gpuTrendAggregate;

  const history = trendClient
    ? await trendClient.findMany({
        where: {
          gpuName,
          bucketStartUtc: {
            gte: since,
          },
        },
        orderBy: {
          bucketStartUtc: "asc",
        },
        take: 48,
      })
    : [];

  return (
    <main className="mx-auto w-full max-w-5xl p-6 md:p-10">
      <header className="mb-5">
        <h1 className="text-2xl font-semibold">{gpuName}</h1>
        <p className="text-sm text-zinc-600">24h UTC half-hour trend (price, utilization, availability ratio).</p>
        <Link className="text-sm text-blue-700 underline" href="/market">
          Back to Market
        </Link>
      </header>

      {history.length === 0 ? (
        <section className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm">
          No 24h trend points yet for this GPU.
        </section>
      ) : (
        <div className="overflow-x-auto rounded border border-zinc-200">
          <table className="min-w-full divide-y divide-zinc-200 text-sm">
            <thead className="bg-zinc-50 text-left text-zinc-700">
              <tr>
                <th className="px-4 py-3 font-medium">UTC Bucket</th>
                <th className="px-4 py-3 font-medium">Source</th>
                <th className="px-4 py-3 font-medium">Snapshots</th>
                <th className="px-4 py-3 font-medium">Total</th>
                <th className="px-4 py-3 font-medium">Utilization</th>
                <th className="px-4 py-3 font-medium">Availability</th>
                <th className="px-4 py-3 font-medium">Median</th>
                <th className="px-4 py-3 font-medium">P90</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {history.map((point) => (
                <tr key={point.id}>
                  <td className="px-4 py-3">{point.bucketStartUtc.toISOString()}</td>
                  <td className="px-4 py-3">{point.source}</td>
                  <td className="px-4 py-3">{point.snapshotCount}</td>
                  <td className="px-4 py-3">{point.totalOffers}</td>
                  <td className="px-4 py-3">{(point.impliedUtilization * 100).toFixed(1)}%</td>
                  <td className="px-4 py-3">{(point.availabilityRatio * 100).toFixed(1)}%</td>
                  <td className="px-4 py-3">
                    {point.medianPrice == null ? "-" : `$${point.medianPrice.toFixed(3)}/hr`}
                  </td>
                  <td className="px-4 py-3">
                    {point.p90Price == null ? "-" : `$${point.p90Price.toFixed(3)}/hr`}
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

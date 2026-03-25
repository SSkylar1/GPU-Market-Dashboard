"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";

type TrendPoint = {
  id: string;
  source: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  bucketStartUtc: string;
  snapshotCount: number;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  minPrice: number | null;
  p10Price: number | null;
  medianPrice: number | null;
  p90Price: number | null;
  newOfferCount: number | null;
  disappearedOfferCount: number | null;
  newOfferRate: number | null;
  disappearedRate: number | null;
  netSupplyChange: number | null;
  marketPressureScore: number | null;
  lowBandDisappearedRate: number | null;
  midBandDisappearedRate: number | null;
  highBandDisappearedRate: number | null;
};

type GpuMetricsResponse = {
  gpuName: string;
  cohort: {
    numGpus: number | null;
    offerType: string | null;
  };
  points: TrendPoint[];
  trends: {
    window6h: { marketPressureScore: { absoluteChange: number } | null };
    window24h: { marketPressureScore: { absoluteChange: number } | null };
    window7d: { marketPressureScore: { absoluteChange: number } | null };
  };
  competition: {
    distinctHosts: number;
    distinctMachines: number;
    offersPerHost: number;
    offersPerMachine: number;
    topHostShare: number;
    top5HostShare: number;
    hostConcentrationIndex: number;
    avgReliabilityScore: number | null;
  };
  regime: string;
  recommendationLabel: string;
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
  roi: {
    expectedUtilizationEstimate: number;
    expectedDailyRevenue: number;
    estimatedDailyPowerCost: number;
    estimatedDailyMargin: number;
    paybackPeriodDays: number | null;
  } | null;
  latestHostMachineBreakdown: Array<{
    hostId: number | null;
    machineId: number | null;
    totalOffers: number;
    rentableOffers: number;
    rentedOffers: number;
    medianPrice: number | null;
  }>;
};

function fmtPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function fmtUsd(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `$${value.toFixed(3)}/hr`;
}

export default function GpuDetailPage() {
  const routeParams = useParams<{ gpu: string }>();
  const queryParams = useSearchParams();
  const gpuName = decodeURIComponent(routeParams.gpu);
  const cohortNumGpus = queryParams.get("numGpus") ?? "";
  const cohortType = queryParams.get("type") ?? "";

  const [data, setData] = useState<GpuMetricsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const query = useMemo(() => {
    const qs = new URLSearchParams();
    if (cohortNumGpus) qs.set("numGpus", cohortNumGpus);
    if (cohortType) qs.set("type", cohortType);
    return qs.toString();
  }, [cohortNumGpus, cohortType]);

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const response = await fetch(`/api/metrics/gpu/${encodeURIComponent(gpuName)}${query ? `?${query}` : ""}`, {
          cache: "no-store",
        });
        const json = (await response.json()) as GpuMetricsResponse;
        if (!mounted) return;
        setData(json);
      } catch (err) {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to load GPU metrics");
      }
    }
    void load();
    return () => {
      mounted = false;
    };
  }, [gpuName, query]);

  return (
    <main className="mx-auto w-full max-w-6xl p-6 md:p-10">
      <header className="mb-5">
        <h1 className="text-2xl font-semibold">{gpuName}</h1>
        <p className="text-sm text-zinc-600">
          Cohort: {data?.cohort.numGpus ?? "all"}x · {data?.cohort.offerType ?? "all"}
        </p>
        <p className="text-sm text-zinc-600">
          Offer churn and unavailable share are market microstructure proxies, not direct rental telemetry.
        </p>
        <Link className="text-sm text-blue-700 underline" href="/market">
          Back to Market
        </Link>
      </header>

      {error ? <p className="mb-4 text-sm text-red-600">{error}</p> : null}

      <section className="mb-6 grid gap-3 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-900 md:grid-cols-2">
        <p>Regime: <strong>{data?.regime ?? "-"}</strong></p>
        <p>Recommendation: <strong>{data?.recommendationLabel ?? "-"}</strong></p>
        <p>Reason: {data?.recommendationReasonPrimary ?? "-"}</p>
        <p>Secondary: {data?.recommendationReasonSecondary ?? "-"}</p>
        <p>Top Host Share: {fmtPct(data?.competition?.topHostShare)}</p>
        <p>Offers / Host: {data?.competition?.offersPerHost?.toFixed(2) ?? "-"}</p>
        <p>Offers / Machine: {data?.competition?.offersPerMachine?.toFixed(2) ?? "-"}</p>
        <p>
          Expected Utilization Estimate: {fmtPct(data?.roi?.expectedUtilizationEstimate)}
        </p>
        <p>Expected Daily Margin: {data?.roi == null ? "-" : `$${data.roi.estimatedDailyMargin.toFixed(2)}`}</p>
        <p>Payback: {data?.roi?.paybackPeriodDays == null ? "-" : `${data.roi.paybackPeriodDays.toFixed(1)} days`}</p>
        <p>Confidence Note: {data?.recommendationConfidenceNote ?? "-"}</p>
      </section>

      <div className="overflow-x-auto rounded border border-zinc-200">
        <table className="min-w-full divide-y divide-zinc-200 text-sm">
          <thead className="bg-zinc-50 text-left text-zinc-700">
            <tr>
              <th className="px-4 py-3 font-medium">UTC Bucket</th>
              <th className="px-4 py-3 font-medium">Total</th>
              <th className="px-4 py-3 font-medium">Unavailable % (Proxy)</th>
              <th className="px-4 py-3 font-medium">New Rate</th>
              <th className="px-4 py-3 font-medium">Disappeared Rate</th>
              <th className="px-4 py-3 font-medium">Net Supply Δ</th>
              <th className="px-4 py-3 font-medium">Pressure</th>
              <th className="px-4 py-3 font-medium">P10</th>
              <th className="px-4 py-3 font-medium">Median</th>
              <th className="px-4 py-3 font-medium">P90</th>
              <th className="px-4 py-3 font-medium">Band Disappear (L/M/H)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-100">
            {(data?.points ?? []).map((point) => (
              <tr key={point.id}>
                <td className="px-4 py-3">{new Date(point.bucketStartUtc).toISOString()}</td>
                <td className="px-4 py-3">{point.totalOffers}</td>
                <td className="px-4 py-3">{fmtPct(point.totalOffers === 0 ? 0 : 1 - point.rentableOffers / point.totalOffers)}</td>
                <td className="px-4 py-3">{fmtPct(point.newOfferRate)}</td>
                <td className="px-4 py-3">{fmtPct(point.disappearedRate)}</td>
                <td className="px-4 py-3">{point.netSupplyChange ?? "-"}</td>
                <td className="px-4 py-3">{point.marketPressureScore == null ? "-" : point.marketPressureScore.toFixed(1)}</td>
                <td className="px-4 py-3">{fmtUsd(point.p10Price)}</td>
                <td className="px-4 py-3">{fmtUsd(point.medianPrice)}</td>
                <td className="px-4 py-3">{fmtUsd(point.p90Price)}</td>
                <td className="px-4 py-3">
                  {fmtPct(point.lowBandDisappearedRate)} / {fmtPct(point.midBandDisappearedRate)} / {fmtPct(point.highBandDisappearedRate)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <section className="mt-6">
        <h2 className="mb-2 text-lg font-medium">Latest Snapshot Host/Machine Breakdown</h2>
        <div className="overflow-x-auto rounded border border-zinc-200">
          <table className="min-w-full divide-y divide-zinc-200 text-sm">
            <thead className="bg-zinc-50 text-left text-zinc-700">
              <tr>
                <th className="px-4 py-3 font-medium">Host ID</th>
                <th className="px-4 py-3 font-medium">Machine ID</th>
                <th className="px-4 py-3 font-medium">Total</th>
                <th className="px-4 py-3 font-medium">Rentable</th>
                <th className="px-4 py-3 font-medium">Lease Signal</th>
                <th className="px-4 py-3 font-medium">Median Price</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {(data?.latestHostMachineBreakdown ?? []).map((row) => (
                <tr key={`${row.hostId ?? "x"}-${row.machineId ?? "x"}`}>
                  <td className="px-4 py-3">{row.hostId ?? "-"}</td>
                  <td className="px-4 py-3">{row.machineId ?? "-"}</td>
                  <td className="px-4 py-3">{row.totalOffers}</td>
                  <td className="px-4 py-3">{row.rentableOffers}</td>
                  <td className="px-4 py-3">{row.rentedOffers}</td>
                  <td className="px-4 py-3">{fmtUsd(row.medianPrice)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

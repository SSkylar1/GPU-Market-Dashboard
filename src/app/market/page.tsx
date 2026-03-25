"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type MetricTrend = {
  latest: number;
  trailingAverage: number;
  absoluteChange: number;
  percentChange: number | null;
  slopePerBucket: number;
  direction: "up" | "down" | "flat";
};

type WindowTrendSummary = {
  pointCount: number;
  supply: MetricTrend | null;
  disappearedRate: MetricTrend | null;
  newOfferRate: MetricTrend | null;
  medianPrice: MetricTrend | null;
  rentableShare: MetricTrend | null;
  marketPressureScore: MetricTrend | null;
};

type RollupRow = {
  id: string;
  gpuName: string;
  numGpus: number;
  offerType: string;
  totalOffers: number;
  rentableOffers: number;
  impliedUtilization: number;
  medianPrice: number | null;
  p10Price: number | null;
  p90Price: number | null;
  latestMarketPressure: number | null;
  regime: "tight" | "balanced" | "oversupplied";
  recommendationLabel: string;
  recommendationReasonPrimary: string;
  competition: {
    topHostShare: number;
    offersPerHost: number;
    offersPerMachine: number;
  };
  churn: {
    latestDisappearedOffers: number;
    newOfferRate: number;
    disappearedRate: number;
    netSupplyChange: number;
  };
  priceBands: {
    lowBandDisappearedRate: number;
    midBandDisappearedRate: number;
    highBandDisappearedRate: number;
  };
  trends: {
    window6h: WindowTrendSummary | null;
    window24h: WindowTrendSummary | null;
    window7d: WindowTrendSummary | null;
  };
  roi: {
    expectedUtilizationEstimate: number;
    paybackPeriodDays: number | null;
    expectedDailyMargin: number;
  };
};

type MetricsResponse = {
  capturedAt: string | null;
  latestAggregateBucketUtc: string | null;
  freshnessMinutes: number | null;
  rollups: RollupRow[];
};

function fmtPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function fmtSigned(value: number | null | undefined, suffix = ""): string {
  if (value == null || !Number.isFinite(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}${suffix}`;
}

function fmtUsd(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `$${value.toFixed(3)}/hr`;
}

export default function MarketPage() {
  const [data, setData] = useState<MetricsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const response = await fetch("/api/metrics", { cache: "no-store" });
        const json = (await response.json()) as MetricsResponse;
        if (!mounted) return;
        setData(json);
      } catch (err) {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to load metrics");
      }
    }
    void load();
    const timer = setInterval(load, 30_000);
    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, []);

  const rows = useMemo(() => data?.rollups ?? [], [data]);

  return (
    <main className="mx-auto w-full max-w-7xl p-6 md:p-10">
      <header className="mb-6 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">GPU Market Dashboard</h1>
          <p className="text-sm text-zinc-600">Latest snapshot: {data?.capturedAt ?? "-"}</p>
          <p className="text-sm text-zinc-600">
            Latest trend bucket (UTC): {data?.latestAggregateBucketUtc ?? "-"}
            {data?.freshnessMinutes == null ? "" : ` · Freshness ${data.freshnessMinutes}m`}
          </p>
          <p className="text-sm text-zinc-600">
            Unavailable, pressure, and utilization outputs are inference proxies from snapshot microstructure.
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

      {error ? (
        <section className="mb-4 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-700">{error}</section>
      ) : null}

      <div className="overflow-x-auto rounded border border-zinc-200">
        <table className="min-w-full divide-y divide-zinc-200 text-sm">
          <thead className="bg-zinc-50 text-left text-zinc-700">
            <tr>
              <th className="px-4 py-3 font-medium">GPU</th>
              <th className="px-4 py-3 font-medium">Count</th>
              <th className="px-4 py-3 font-medium">Type</th>
              <th className="px-4 py-3 font-medium">Latest Supply</th>
              <th className="px-4 py-3 font-medium">Unavailable % (Proxy)</th>
              <th className="px-4 py-3 font-medium">Market Pressure</th>
              <th className="px-4 py-3 font-medium">Regime</th>
              <th className="px-4 py-3 font-medium">24h Supply Δ</th>
              <th className="px-4 py-3 font-medium">24h Price Δ%</th>
              <th className="px-4 py-3 font-medium">24h Pressure Δ</th>
              <th className="px-4 py-3 font-medium">Band Clearing (L/M/H)</th>
              <th className="px-4 py-3 font-medium">Top Host Share</th>
              <th className="px-4 py-3 font-medium">Expected Util. Est.</th>
              <th className="px-4 py-3 font-medium">Payback (days)</th>
              <th className="px-4 py-3 font-medium">Reco</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-100">
            {rows.map((row) => (
              <tr key={row.id}>
                <td className="px-4 py-3">
                  <Link
                    className="text-blue-700 underline"
                    href={`/gpus/${encodeURIComponent(row.gpuName)}?numGpus=${row.numGpus}&type=${encodeURIComponent(row.offerType)}`}
                  >
                    {row.gpuName}
                  </Link>
                </td>
                <td className="px-4 py-3">{row.numGpus}</td>
                <td className="px-4 py-3">{row.offerType}</td>
                <td className="px-4 py-3">{row.totalOffers}</td>
                <td className="px-4 py-3">{fmtPct(row.impliedUtilization)}</td>
                <td className="px-4 py-3">{row.latestMarketPressure == null ? "-" : row.latestMarketPressure.toFixed(1)}</td>
                <td className="px-4 py-3">{row.regime}</td>
                <td className="px-4 py-3">{fmtSigned(row.trends.window24h?.supply?.absoluteChange, "")}</td>
                <td className="px-4 py-3">
                  {row.trends.window24h?.medianPrice?.percentChange == null
                    ? "-"
                    : `${row.trends.window24h.medianPrice.percentChange > 0 ? "+" : ""}${row.trends.window24h.medianPrice.percentChange.toFixed(1)}%`}
                </td>
                <td className="px-4 py-3">{fmtSigned(row.trends.window24h?.marketPressureScore?.absoluteChange)}</td>
                <td className="px-4 py-3">
                  {fmtPct(row.priceBands.lowBandDisappearedRate)} / {fmtPct(row.priceBands.midBandDisappearedRate)} / {fmtPct(row.priceBands.highBandDisappearedRate)}
                </td>
                <td className="px-4 py-3">{fmtPct(row.competition.topHostShare)}</td>
                <td className="px-4 py-3">{fmtPct(row.roi.expectedUtilizationEstimate)}</td>
                <td className="px-4 py-3">
                  {row.roi.paybackPeriodDays == null ? "-" : `${row.roi.paybackPeriodDays.toFixed(0)}`}
                </td>
                <td className="px-4 py-3" title={row.recommendationReasonPrimary}>{row.recommendationLabel}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="mt-3 text-xs text-zinc-500">
        Price-band clearing is based on disappeared offers by low/mid/high cohort-relative bands. This is a market-tightening proxy, not a direct rental feed.
      </p>
    </main>
  );
}

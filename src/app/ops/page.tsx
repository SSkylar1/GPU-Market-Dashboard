"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type HealthLevel = "healthy" | "warning" | "critical";

type OpsHealthResponse = {
  nowUtc: string;
  sourceScope: string;
  overallHealth: HealthLevel;
  expectedCadenceMinutes: number;
  latestSnapshot: {
    id: string;
    source: string;
    ingestMode: string | null;
    capturedAt: string;
    createdAt: string;
    lagMinutes: number | null;
    health: HealthLevel;
  } | null;
  latestAggregate: {
    bucketStartUtc: string;
    updatedAt: string;
    bucketLagMinutes: number | null;
    lagMinutes: number | null;
    health: HealthLevel;
  } | null;
  cadence: {
    health: HealthLevel;
    snapshots6h: number;
    snapshots24h: number;
    avgGapMinutes: number | null;
    maxGapMinutes: number | null;
  };
  sourceMix: {
    snapshotsBySource6h: Record<string, number>;
    snapshotsBySource24h: Record<string, number>;
  };
  latestScenarioForecast: {
    id: string;
    createdAt: string;
    source: string;
    modelVersion: string;
  } | null;
};

function badgeClass(level: HealthLevel): string {
  if (level === "healthy") return "border-emerald-500/50 bg-emerald-950/40 text-emerald-200";
  if (level === "warning") return "border-amber-500/50 bg-amber-950/40 text-amber-200";
  return "border-red-500/50 bg-red-950/40 text-red-200";
}

export default function OpsPage() {
  const [data, setData] = useState<OpsHealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const response = await fetch("/api/ops/health?source=vast-live", { cache: "no-store" });
        const json = (await response.json()) as OpsHealthResponse;
        if (!mounted) return;
        setData(json);
        setError(null);
      } catch (err) {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to load ops health");
      }
    }
    void load();
    const timer = setInterval(load, 30_000);
    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, []);

  const sourceRows = useMemo(() => {
    if (!data) return [];
    const keys = new Set([
      ...Object.keys(data.sourceMix.snapshotsBySource6h),
      ...Object.keys(data.sourceMix.snapshotsBySource24h),
    ]);
    return [...keys]
      .sort((a, b) => a.localeCompare(b))
      .map((source) => ({
        source,
        snapshots6h: data.sourceMix.snapshotsBySource6h[source] ?? 0,
        snapshots24h: data.sourceMix.snapshotsBySource24h[source] ?? 0,
      }));
  }, [data]);

  return (
    <main className="mx-auto w-full max-w-6xl p-6 text-zinc-100 md:p-10">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Operations</h1>
          <p className="text-sm text-zinc-400">Live pipeline and freshness health checks.</p>
          <p className="text-xs text-zinc-500">Scope: {data?.sourceScope ?? "vast-live"}</p>
        </div>
        <nav className="flex gap-3 text-sm">
          <Link className="text-blue-400 underline" href="/market">
            Market
          </Link>
          <Link className="text-blue-400 underline" href="/scoring">
            Scoring
          </Link>
          <Link className="text-blue-400 underline" href="/pricing">
            Pricing
          </Link>
        </nav>
      </header>

      {error ? (
        <section className="mb-4 rounded border border-red-500/60 bg-red-950/30 p-3 text-red-200">{error}</section>
      ) : null}

      <section className="mb-4 grid gap-3 md:grid-cols-4">
        <div className={`rounded border p-3 ${badgeClass(data?.overallHealth ?? "critical")}`}>
          <p className="text-xs uppercase tracking-wide">Overall</p>
          <p className="text-lg font-semibold">{data?.overallHealth ?? "unknown"}</p>
        </div>
        <div className={`rounded border p-3 ${badgeClass(data?.latestSnapshot?.health ?? "critical")}`}>
          <p className="text-xs uppercase tracking-wide">Snapshot Freshness</p>
          <p className="text-lg font-semibold">
            {data?.latestSnapshot?.lagMinutes == null ? "n/a" : `${data.latestSnapshot.lagMinutes}m`}
          </p>
        </div>
        <div className={`rounded border p-3 ${badgeClass(data?.latestAggregate?.health ?? "critical")}`}>
          <p className="text-xs uppercase tracking-wide">Aggregate Freshness</p>
          <p className="text-lg font-semibold">
            {data?.latestAggregate?.lagMinutes == null ? "n/a" : `${data.latestAggregate.lagMinutes}m`}
          </p>
        </div>
        <div className={`rounded border p-3 ${badgeClass(data?.cadence.health ?? "warning")}`}>
          <p className="text-xs uppercase tracking-wide">Cadence</p>
          <p className="text-lg font-semibold">
            avg {data?.cadence.avgGapMinutes == null ? "n/a" : `${data.cadence.avgGapMinutes}m`}
          </p>
        </div>
      </section>

      <section className="mb-4 rounded border border-zinc-700 bg-zinc-900 p-4">
        <h2 className="mb-3 text-base font-semibold">Pipeline Details</h2>
        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded border border-zinc-700 bg-zinc-950 p-3 text-sm">
            <p>Expected cadence: every {data?.expectedCadenceMinutes ?? 5} minutes</p>
            <p>Snapshots in last 6h: {data?.cadence.snapshots6h ?? 0}</p>
            <p>Snapshots in last 24h: {data?.cadence.snapshots24h ?? 0}</p>
            <p>Max gap (6h): {data?.cadence.maxGapMinutes == null ? "n/a" : `${data.cadence.maxGapMinutes}m`}</p>
          </div>
          <div className="rounded border border-zinc-700 bg-zinc-950 p-3 text-sm">
            <p>Latest snapshot: {data?.latestSnapshot?.id ?? "none"}</p>
            <p>Source/mode: {data?.latestSnapshot ? `${data.latestSnapshot.source} / ${data.latestSnapshot.ingestMode ?? "unknown"}` : "-"}</p>
            <p>Captured UTC: {data?.latestSnapshot?.capturedAt ?? "-"}</p>
            <p>Latest aggregate bucket UTC: {data?.latestAggregate?.bucketStartUtc ?? "-"}</p>
            <p>Aggregate bucket age: {data?.latestAggregate?.bucketLagMinutes == null ? "n/a" : `${data.latestAggregate.bucketLagMinutes}m`}</p>
          </div>
        </div>
      </section>

      <section className="rounded border border-zinc-700 bg-zinc-900 p-4">
        <h2 className="mb-3 text-base font-semibold">Source Activity</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="text-zinc-400">
              <tr>
                <th className="px-3 py-2 text-left">Source</th>
                <th className="px-3 py-2 text-left">Snapshots (6h)</th>
                <th className="px-3 py-2 text-left">Snapshots (24h)</th>
              </tr>
            </thead>
            <tbody>
              {sourceRows.length === 0 ? (
                <tr>
                  <td className="px-3 py-2 text-zinc-400" colSpan={3}>
                    No source activity yet.
                  </td>
                </tr>
              ) : (
                sourceRows.map((row) => (
                  <tr key={row.source} className="border-t border-zinc-800">
                    <td className="px-3 py-2">{row.source}</td>
                    <td className="px-3 py-2">{row.snapshots6h}</td>
                    <td className="px-3 py-2">{row.snapshots24h}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

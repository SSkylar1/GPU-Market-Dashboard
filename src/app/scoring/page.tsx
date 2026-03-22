"use client";

import { useEffect, useMemo, useState } from "react";

type ScoringResponse = {
  overallScore: number;
  recommendation: "Buy" | "Watch" | "Avoid";
  demandScore: number;
  competitionScore: number;
  priceStrengthScore: number;
  efficiencyScore: number;
  expectedDailyRevenue: number;
  expectedDailyPowerCost: number;
  expectedDailyProfit: number;
  expectedPaybackMonths: number | null;
  confidence: {
    level: "low" | "medium" | "high";
    bucketCount: number;
    score: number;
    leaseSignalQuality: "low" | "high";
  };
  marketSignals: {
    availableShare: number;
    unavailableShare: number;
    activeLeaseShare: number;
    elasticityAvailPtsPerDollar: number | null;
    leaseSignalQuality: "low" | "high";
  };
  pricing: {
    aggressive: number;
    target: number;
    premium: number;
  };
  scenarioId: string;
  scenarioScoreId: string;
};

type FormState = {
  gpuName: string;
  gpuCount: number;
  assumedPowerWatts: number;
  assumedHardwareCost: number;
  electricityCostPerKwh: number;
  targetPaybackMonths: number;
  source: string;
  hoursWindow: number;
};

type GpuOption = {
  gpuName: string;
  source: string;
  latestMedianPrice: number | null;
  latestImpliedUtilization: number | null;
  latestBucketUtc: string | null;
  defaults: {
    assumedPowerWatts: number;
    assumedHardwareCost: number;
    gpuCount: number;
    electricityCostPerKwh: number;
    targetPaybackMonths: number;
    hoursWindow: number;
  };
};

type RecentScenario = {
  id: string;
  gpuName: string;
  gpuCount: number;
  createdAt: string;
  targetPaybackMonths: number;
  latestScore: {
    overallScore: number;
    recommendation: string;
    createdAt: string;
  } | null;
};

type ScoringMetaResponse = {
  gpuOptions: GpuOption[];
  recentScenarios: RecentScenario[];
};

const defaultForm: FormState = {
  gpuName: "",
  gpuCount: 1,
  assumedPowerWatts: 450,
  assumedHardwareCost: 2500,
  electricityCostPerKwh: 0.12,
  targetPaybackMonths: 18,
  source: "vast-live",
  hoursWindow: 24,
};

export default function ScoringPage() {
  const [form, setForm] = useState<FormState>(defaultForm);
  const [loading, setLoading] = useState(false);
  const [metaLoading, setMetaLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ScoringResponse | null>(null);
  const [gpuOptions, setGpuOptions] = useState<GpuOption[]>([]);
  const [recentScenarios, setRecentScenarios] = useState<RecentScenario[]>([]);

  useEffect(() => {
    async function loadMeta() {
      setMetaLoading(true);
      try {
        const response = await fetch("/api/scoring", { cache: "no-store" });
        const data = (await response.json()) as ScoringMetaResponse;
        setGpuOptions(data.gpuOptions ?? []);
        setRecentScenarios(data.recentScenarios ?? []);

        if ((data.gpuOptions ?? []).length > 0) {
          const first = data.gpuOptions[0];
          setForm((current) => ({
            ...current,
            gpuName: first.gpuName,
            source: first.source,
            gpuCount: first.defaults.gpuCount,
            assumedPowerWatts: first.defaults.assumedPowerWatts,
            assumedHardwareCost: first.defaults.assumedHardwareCost,
            electricityCostPerKwh: first.defaults.electricityCostPerKwh,
            targetPaybackMonths: first.defaults.targetPaybackMonths,
            hoursWindow: first.defaults.hoursWindow,
          }));
        }
      } catch (metaError) {
        setError(metaError instanceof Error ? metaError.message : "Failed to load scoring metadata");
      } finally {
        setMetaLoading(false);
      }
    }

    void loadMeta();
  }, []);

  const selectedOption = useMemo(
    () => gpuOptions.find((option) => option.gpuName === form.gpuName) ?? null,
    [gpuOptions, form.gpuName],
  );

  const confidenceClass = useMemo(() => {
    if (!result) return "";
    if (result.confidence.level === "high") return "text-emerald-400";
    if (result.confidence.level === "medium") return "text-amber-400";
    return "text-red-400";
  }, [result]);

  async function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const response = await fetch("/api/scoring", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(form),
      });

      const data = (await response.json()) as ScoringResponse | { error: string };
      if (!response.ok || "error" in data) {
        setResult(null);
        setError("error" in data ? data.error : "Scoring failed");
      } else {
        setResult(data);
      }
    } catch (requestError) {
      setResult(null);
      setError(requestError instanceof Error ? requestError.message : "Request failed");
    } finally {
      setLoading(false);
    }
  }

  function updateNumber<K extends keyof FormState>(key: K, value: string) {
    setForm((current) => ({
      ...current,
      [key]: value === "" ? 0 : Number(value),
    }));
  }

  function onGpuChange(gpuName: string) {
    const option = gpuOptions.find((candidate) => candidate.gpuName === gpuName);
    if (!option) {
      setForm((current) => ({ ...current, gpuName }));
      return;
    }

    setForm((current) => ({
      ...current,
      gpuName: option.gpuName,
      source: option.source,
      gpuCount: option.defaults.gpuCount,
      assumedPowerWatts: option.defaults.assumedPowerWatts,
      assumedHardwareCost: option.defaults.assumedHardwareCost,
      electricityCostPerKwh: option.defaults.electricityCostPerKwh,
      targetPaybackMonths: option.defaults.targetPaybackMonths,
      hoursWindow: option.defaults.hoursWindow,
    }));
  }

  return (
    <main className="mx-auto w-full max-w-6xl p-6 md:p-10">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-100">Scenario Scoring</h1>

      <form onSubmit={onSubmit} className="mb-6 grid gap-4 rounded border border-zinc-700 bg-zinc-900 p-4 text-sm text-zinc-100 md:grid-cols-2">
        <label className="flex flex-col gap-1">
          GPU Name
          <select
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.gpuName}
            onChange={(event) => onGpuChange(event.target.value)}
            disabled={metaLoading || gpuOptions.length === 0}
          >
            {gpuOptions.map((option) => (
              <option key={option.gpuName} value={option.gpuName}>
                {option.gpuName}
              </option>
            ))}
          </select>
        </label>

        <label className="flex flex-col gap-1">
          Source
          <input
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.source}
            onChange={(event) => setForm((current) => ({ ...current, source: event.target.value }))}
          />
        </label>

        <label className="flex flex-col gap-1">
          GPU Count
          <input
            type="number"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.gpuCount}
            onChange={(event) => updateNumber("gpuCount", event.target.value)}
          />
        </label>

        <label className="flex flex-col gap-1">
          Power (Watts)
          <input
            type="number"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.assumedPowerWatts}
            onChange={(event) => updateNumber("assumedPowerWatts", event.target.value)}
          />
        </label>

        <label className="flex flex-col gap-1">
          Hardware Cost ($)
          <input
            type="number"
            step="0.01"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.assumedHardwareCost}
            onChange={(event) => updateNumber("assumedHardwareCost", event.target.value)}
          />
        </label>

        <label className="flex flex-col gap-1">
          Electricity ($/kWh)
          <input
            type="number"
            step="0.01"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.electricityCostPerKwh}
            onChange={(event) => updateNumber("electricityCostPerKwh", event.target.value)}
          />
        </label>

        <label className="flex flex-col gap-1">
          Target Payback (months)
          <input
            type="number"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.targetPaybackMonths}
            onChange={(event) => updateNumber("targetPaybackMonths", event.target.value)}
          />
        </label>

        <label className="flex flex-col gap-1">
          Hours Window
          <input
            type="number"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.hoursWindow}
            onChange={(event) => updateNumber("hoursWindow", event.target.value)}
          />
        </label>

        <div className="md:col-span-2 text-xs text-zinc-300">
          {selectedOption ? (
            <span>
              Latest {selectedOption.source} median: {selectedOption.latestMedianPrice == null ? "-" : `$${selectedOption.latestMedianPrice.toFixed(3)}/hr`} ·
              Utilization: {selectedOption.latestImpliedUtilization == null ? "-" : `${(selectedOption.latestImpliedUtilization * 100).toFixed(1)}%`} ·
              Bucket: {selectedOption.latestBucketUtc ?? "-"}
            </span>
          ) : (
            <span>No GPU options available yet. Run live collection first.</span>
          )}
        </div>

        <div className="md:col-span-2">
          <button
            type="submit"
            disabled={loading || metaLoading || gpuOptions.length === 0}
            className="rounded bg-blue-600 px-4 py-2 font-medium text-white disabled:opacity-60"
          >
            {loading ? "Scoring..." : "Run Market Score"}
          </button>
        </div>
      </form>

      {error ? (
        <section className="mb-4 rounded border border-red-500 bg-red-950/40 p-3 text-sm text-red-200">{error}</section>
      ) : null}

      {result ? (
        <section className="mb-6 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-900">
          <h2 className="mb-2 text-lg font-semibold">Result</h2>
          <p>Overall Score: {result.overallScore}</p>
          <p>Recommendation: {result.recommendation}</p>
          <p className={confidenceClass}>
            Confidence: {result.confidence.level} ({result.confidence.score.toFixed(1)}/100, {result.confidence.bucketCount} buckets, lease signal {result.confidence.leaseSignalQuality})
          </p>

          <div className="mt-3 grid gap-1 md:grid-cols-2">
            <p>Demand: {result.demandScore.toFixed(2)}</p>
            <p>Competition: {result.competitionScore.toFixed(2)}</p>
            <p>Price Strength: {result.priceStrengthScore.toFixed(2)}</p>
            <p>Efficiency: {result.efficiencyScore.toFixed(2)}</p>
            <p>Available Share: {(result.marketSignals.availableShare * 100).toFixed(1)}%</p>
            <p>Unavailable Share: {(result.marketSignals.unavailableShare * 100).toFixed(1)}%</p>
            <p>Lease Signal Share: {(result.marketSignals.activeLeaseShare * 100).toFixed(1)}%</p>
            <p>
              Price Elasticity (avail pts/$): {result.marketSignals.elasticityAvailPtsPerDollar == null ? "-" : result.marketSignals.elasticityAvailPtsPerDollar.toFixed(3)}
            </p>
            <p>Daily Revenue: ${result.expectedDailyRevenue.toFixed(2)}</p>
            <p>Daily Power Cost: ${result.expectedDailyPowerCost.toFixed(2)}</p>
            <p>Daily Profit: ${result.expectedDailyProfit.toFixed(2)}</p>
            <p>
              Payback: {result.expectedPaybackMonths == null ? "Not achievable" : `${result.expectedPaybackMonths.toFixed(2)} months`}
            </p>
            <p>Price Aggressive: ${result.pricing.aggressive.toFixed(3)}/hr</p>
            <p>Price Target: ${result.pricing.target.toFixed(3)}/hr</p>
            <p>Price Premium: ${result.pricing.premium.toFixed(3)}/hr</p>
            <p>Scenario ID: {result.scenarioId}</p>
          </div>
        </section>
      ) : null}

      <section className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-900">
        <h2 className="mb-2 text-lg font-semibold">Recent Scored Scenarios</h2>
        {recentScenarios.length === 0 ? (
          <p>No scenarios saved yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-zinc-200">
              <thead className="bg-zinc-100 text-left text-zinc-700">
                <tr>
                  <th className="px-3 py-2 font-medium">GPU</th>
                  <th className="px-3 py-2 font-medium">Count</th>
                  <th className="px-3 py-2 font-medium">Target Payback</th>
                  <th className="px-3 py-2 font-medium">Latest Score</th>
                  <th className="px-3 py-2 font-medium">Recommendation</th>
                  <th className="px-3 py-2 font-medium">Created</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-200">
                {recentScenarios.map((scenario) => (
                  <tr key={scenario.id}>
                    <td className="px-3 py-2">{scenario.gpuName}</td>
                    <td className="px-3 py-2">{scenario.gpuCount}</td>
                    <td className="px-3 py-2">{scenario.targetPaybackMonths}m</td>
                    <td className="px-3 py-2">
                      {scenario.latestScore ? scenario.latestScore.overallScore.toFixed(2) : "-"}
                    </td>
                    <td className="px-3 py-2">{scenario.latestScore?.recommendation ?? "-"}</td>
                    <td className="px-3 py-2">{new Date(scenario.createdAt).toISOString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

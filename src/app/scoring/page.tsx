"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Area,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  BarChart,
  Bar,
} from "recharts";
import { filterSeriesByRangeHours } from "@/lib/scoring/chartTransforms";

type GpuOption = {
  gpuName: string;
  cohortNumGpus: number | null;
  cohortOfferType: string | null;
  source: string;
  latestMedianPrice: number | null;
  latestImpliedUtilization: number | null;
  latestBucketUtc: string | null;
  latestPressure: number | null;
  latestState: string | null;
  latestStateConfidence: number | null;
  latestUniqueMachines: number | null;
  latestInferabilityScore?: number | null;
  latestSignalStrengthScore?: number | null;
  latestIdentityQualityScore?: number | null;
  defaults: {
    assumedPowerWatts: number;
    assumedHardwareCost: number;
    gpuCount: number;
    electricityCostPerKwh: number;
    targetPaybackMonths: number;
    hoursWindow: number;
  };
};

type CohortUniverseEntry = {
  key: string;
  gpuName: string;
  cohortNumGpus: number | null;
  cohortOfferType: string | null;
  recommendation: "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy";
  state: string;
  confidence: number;
  inferability: number;
  readinessScore: number;
  readinessBand: "Too early" | "Emerging signal" | "Usable with caution" | "Decision-grade";
  readinessTags: string[];
  pressure: number;
  signalStrengthScore: number;
  samplingQualityScore: number;
  identityQualityScore: number;
  lifecycleObservabilityScore: number;
  observationCount: number;
  observationsPerOffer: number;
  medianPollGapMinutes: number;
  maxPollGapMinutes: number;
  coverageRatio: number;
  churnScore: number;
  insufficientSampling: boolean;
  exploratoryOpportunityScore: number;
  exploratoryRank: number;
  globalRank: number;
  nearestUpgrade: "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy" | null;
  nearestDowngrade: "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy" | null;
  upgradeGuidance: string[];
  downgradeRiskFactors: string[];
  inferabilityDecomposition: {
    marketAmbiguity: number;
    poorSampling: number;
    weakIdentity: number;
    thinDepth: number;
    churnReappearance: number;
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
  confidenceScore: number;
  expectedUtilization: number;
  expectedPaybackMonths: number | null;
};

type ScoringMetaResponse = {
  gpuOptions: GpuOption[];
  cohortUniverse: CohortUniverseEntry[];
  recommendationDistribution: Record<string, number>;
  regimeDistribution: Record<string, number>;
  suppressionSummary: {
    suppressedCount: number;
    nearUsableCount: number;
    underSampledCount: number;
    identityConstrainedCount: number;
    churnHeavyCount: number;
    graduatingSoonCount: number;
  };
  pollingSummary: {
    highPriorityTargetMinutes: string;
    generalTargetMinutes: string;
    longTailTargetMinutes: string;
  };
  recentScenarios: RecentScenario[];
};

type ForecastResponse = {
  recommendation: string;
  recommendationReasonPrimary: string;
  recommendationReasonSecondary: string;
  recommendationConfidenceNote: string;
  forecastSuppressed: boolean;
  vetoReason: "non_inferable" | "low_inferability" | "identity_quality" | "churn_dominated" | null;
  currentState: {
    state: string;
    pressure: number;
    movementScore: number;
    confidenceScore: number;
    timeDepthScore: number;
    crossSectionDepthScore: number;
    dataDepthScore: number;
    noiseScore: number;
    churnScore: number;
    signalStrengthScore: number;
    inferabilityScore: number;
    identityQualityScore: number;
  };
  exactCohort: {
    gpuName: string;
    numGpus: number | null;
    offerType: string | null;
    latestBucketUtc: string;
    medianPrice: number | null;
    totalOffers: number;
    uniqueMachines: number;
    uniqueHosts: number;
    machineConcentrationShareTop1: number;
    machineConcentrationShareTop3: number;
    hostConcentrationShareTop1: number;
    hostConcentrationShareTop3: number;
    machinePersistenceRate: number;
    hostPersistenceRate: number;
    newMachineEntryRate: number;
    disappearingMachineRate: number;
    persistentDisappearanceRate: number;
    persistentDisappearanceRateN: number;
    temporaryMissingRate: number;
    reappearedShortGapRate: number;
    reappearedLongGapRate: number;
    medianReappearanceDelayBuckets: number | null;
    churnAdjustedDisappearanceRate: number;
    reappearedRate: number;
  };
  familyBaseline: {
    medianPrice: number | null;
    pressure: number;
    hazard: number;
    machineDepth: number;
    confidenceScore: number;
    inferabilityScore: number;
  };
  forecastProbabilities: {
    pTight24h: number;
    pTight72h: number;
    pTight7d: number;
    pPriceUp24h: number;
    pPriceFlat24h: number;
    pPriceDown24h: number;
    pOfferConsumedWithin12h: number;
    pOfferConsumedWithin24h: number;
    pOfferConsumedWithin72h: number;
    pOfferConsumedWithin12hRaw: number;
    pOfferConsumedWithin24hRaw: number;
    pOfferConsumedWithin72hRaw: number;
    pOfferConsumedWithin12hCalibrated: number;
    pOfferConsumedWithin24hCalibrated: number;
    pOfferConsumedWithin72hCalibrated: number;
  };
  utilization: {
    expected: number;
    low: number;
    high: number;
    pAbove25: number;
    pAbove50: number;
    pAbove75: number;
  };
  economics: {
    listingPricePerHour: number;
    relativePriceVsExactMedian: number;
    relativePriceVsFamilyMedian: number;
    expectedDailyRevenue: number;
    expectedDailyRevenueLow: number;
    expectedDailyRevenueHigh: number;
    expectedDailyMargin: number;
    expectedDailyMarginLow: number;
    expectedDailyMarginHigh: number;
    expectedPaybackMonths: number | null;
    expectedPaybackMonthsLow: number | null;
    expectedPaybackMonthsHigh: number | null;
    pPaybackWithinTarget: number;
    pScenarioOutperformingGpuFamilyMedian: number;
  };
  confidence: {
    score: number;
    level: "low" | "medium" | "high";
    bucketCount: number;
    notes: string[];
    forecastSuppressed: boolean;
    vetoReason: "non_inferable" | "low_inferability" | "identity_quality" | "churn_dominated" | null;
    inferabilityScore: number;
    signalStrengthScore: number;
    identityQualityScore: number;
  };
  observationQuality: {
    observationCount: number;
    observationsPerOffer: number;
    medianPollGapMinutes: number;
    maxPollGapMinutes: number;
    coverageRatio: number;
    offerSeenSpanMinutes: number;
    cohortObservationDensityScore: number;
    labelabilityScore: number;
    futureWindowCoverage12h: number;
    futureWindowCoverage24h: number;
    futureWindowCoverage72h: number;
    samplingQualityScore: number;
    lifecycleObservabilityScore: number;
    insufficientSampling: boolean;
    dataFreshnessQuality: "high" | "medium" | "low";
  };
  inferabilityDecomposition: {
    marketAmbiguity: number;
    poorSampling: number;
    weakIdentity: number;
    thinDepth: number;
    churnReappearance: number;
  };
  readiness: {
    readinessScore: number;
    readinessBand: "Too early" | "Emerging signal" | "Usable with caution" | "Decision-grade";
    readinessBreakdown: Record<string, number>;
    graduationTags: string[];
  };
  suppressionReasons: string[];
  samplingReasons: string[];
  nearestUpgrade: "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy" | null;
  nearestDowngrade: "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy" | null;
  upgradeGuidance: string[];
  downgradeRiskFactors: string[];
  exploratoryOpportunityScore: number;
  displayRecommendationReason: string;
  unsuppressedProbabilities: {
    tight: { p24hRaw: number; p72hRaw: number; p7dRaw: number; p24hConservative: number };
    priceDirection24h: { upRaw: number; flatRaw: number; downRaw: number };
    consumption: {
      p12hRaw: number;
      p24hRaw: number;
      p72hRaw: number;
      p12hCalibrated: number;
      p24hCalibrated: number;
      p72hCalibrated: number;
    };
  };
  compareMetrics: {
    pressure: number;
    readiness: number;
    inferability: number;
    confidence: number;
    samplingQuality: number;
    identityQuality: number;
    lifecycleObservability: number;
    priceAdvantage: number;
    churnPenalty: number;
    pConsumed24h: number;
  };
  explanation: {
    observed: string[];
    inferred: string[];
    forecasted: string[];
    risks: string[];
  };
  visuals: {
    pressureTimeline: Array<{
      bucketStartUtc: string;
      pressure: number;
      state: string;
      confidence: number;
      pressureLow: number;
      pressureHigh: number;
    }>;
    supplyTimeline: Array<{
      bucketStartUtc: string;
      totalOffers: number;
      uniqueMachines: number;
      newOffers: number;
      continuingOffers: number;
      disappearedOffers: number;
      reappearedOffers: number;
    }>;
    offerSurvival: Array<{ durationHoursBucket: string; count: number; priceBand: string }>;
    pricePositionCurve: Array<{
      relativePricePosition: number;
      p12h: number;
      p24h: number;
      p72h: number;
      p12hLow: number;
      p12hHigh: number;
      p24hLow: number;
      p24hHigh: number;
      p72hLow: number;
      p72hHigh: number;
    }>;
    configComparison: Array<{
      numGpus: number;
      offerType: string;
      pressure: number;
      hazard: number;
      medianPrice: number | null;
      uniqueMachines: number;
      confidence: number;
    }>;
    marketMap: Array<{
      label: string;
      expectedUtilization: number;
      expectedPayback: number | null;
      bubble: number;
      confidence: number;
      recommendation: string;
    }>;
    calibration: Array<{ bucket: string; count: number; avgPredicted: number; realizedRate: number }>;
  };
  drilldowns: {
    latestOffers: Array<{
      offerId: string;
      machineId: number | null;
      hostId: number | null;
      pricePerHour: number | null;
      reliabilityScore: number | null;
      rentable: boolean;
    }>;
    machineConcentration: Array<{ machineId: number | null; offers: number; share: number }>;
    cohortComparisons: Array<{
      cohort: string;
      pressure: number;
      medianPrice: number | null;
      uniqueMachines: number;
      state: string;
      confidence: number;
    }>;
    backtestCalibrationSummary: Array<{
      horizonHours: number;
      bucket: string;
      count: number;
      realizedRate: number;
      inferabilityBucket?: string | null;
      stateAtPrediction?: string | null;
    }>;
    labelQualitySummary: Array<{ horizonHours: number; quality: string; count: number }>;
  };
};

type FormState = {
  gpuName: string;
  cohortSelection: string;
  gpuCount: number;
  assumedPowerWatts: number;
  assumedHardwareCost: number;
  electricityCostPerKwh: number;
  targetPaybackMonths: number;
  source: string;
  hoursWindow: number;
  listingPricePerHour: number | "";
};

const defaultForm: FormState = {
  gpuName: "",
  cohortSelection: "combined",
  gpuCount: 1,
  assumedPowerWatts: 450,
  assumedHardwareCost: 2500,
  electricityCostPerKwh: 0.12,
  targetPaybackMonths: 18,
  source: "vast-live",
  hoursWindow: 24 * 7,
  listingPricePerHour: "",
};

function pct(value: number) {
  return `${(value * 100).toFixed(1)}%`;
}

function dollars(value: number | null) {
  if (value == null) return "n/a";
  return `$${value.toFixed(2)}`;
}

type ChartLegendKey = "supply" | "priceCurve" | "config" | "calibration";

export default function ScoringPage() {
  const [form, setForm] = useState<FormState>(defaultForm);
  const [gpuOptions, setGpuOptions] = useState<GpuOption[]>([]);
  const [cohortUniverse, setCohortUniverse] = useState<CohortUniverseEntry[]>([]);
  const [mode, setMode] = useState<"conservative" | "exploratory">("conservative");
  const [recommendationDistribution, setRecommendationDistribution] = useState<Record<string, number>>({});
  const [regimeDistribution, setRegimeDistribution] = useState<Record<string, number>>({});
  const [suppressionSummary, setSuppressionSummary] = useState<ScoringMetaResponse["suppressionSummary"] | null>(null);
  const [pollingSummary, setPollingSummary] = useState<ScoringMetaResponse["pollingSummary"] | null>(null);
  const [selectedCompareKeys, setSelectedCompareKeys] = useState<string[]>([]);
  const [detailKey, setDetailKey] = useState<string | null>(null);
  const [cohortFilter, setCohortFilter] = useState<"all" | "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy">("all");
  const [cohortSort, setCohortSort] = useState<"rank" | "readiness" | "opportunity" | "consumption" | "inferability">("rank");
  const [recentScenarios, setRecentScenarios] = useState<RecentScenario[]>([]);
  const [result, setResult] = useState<ForecastResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [metaLoading, setMetaLoading] = useState(true);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [range, setRange] = useState<"24h" | "72h" | "7d">("7d");
  const [error, setError] = useState<string | null>(null);
  const [legendSelection, setLegendSelection] = useState<Record<ChartLegendKey, string[] | null>>({
    supply: null,
    priceCurve: null,
    config: null,
    calibration: null,
  });

  useEffect(() => {
    async function loadMeta() {
      setMetaLoading(true);
      try {
        const response = await fetch("/api/scoring", { cache: "no-store" });
        const data = (await response.json()) as ScoringMetaResponse;
        setGpuOptions(data.gpuOptions ?? []);
        setCohortUniverse(data.cohortUniverse ?? []);
        setRecommendationDistribution(data.recommendationDistribution ?? {});
        setRegimeDistribution(data.regimeDistribution ?? {});
        setSuppressionSummary(data.suppressionSummary ?? null);
        setPollingSummary(data.pollingSummary ?? null);
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
        setError(metaError instanceof Error ? metaError.message : "Failed to load metadata");
      } finally {
        setMetaLoading(false);
      }
    }

    void loadMeta();
  }, []);

  const gpuNames = useMemo(
    () => Array.from(new Set(gpuOptions.map((option) => option.gpuName))).sort((a, b) => a.localeCompare(b)),
    [gpuOptions],
  );

  const cohortOptions = useMemo(() => {
    if (!form.gpuName) return [];
    return gpuOptions
      .filter((option) => option.gpuName === form.gpuName)
      .sort((a, b) => {
        const left = a.cohortNumGpus ?? 0;
        const right = b.cohortNumGpus ?? 0;
        if (left !== right) return left - right;
        return (a.cohortOfferType ?? "").localeCompare(b.cohortOfferType ?? "");
      });
  }, [gpuOptions, form.gpuName]);

  const selectedOption = useMemo(() => {
    if (!form.gpuName || form.cohortSelection === "combined") return null;
    const [numRaw, offerType] = form.cohortSelection.split("::");
    const num = Number(numRaw);
    return (
      gpuOptions.find(
        (option) =>
          option.gpuName === form.gpuName &&
          option.cohortNumGpus === num &&
          (option.cohortOfferType ?? "unknown") === offerType,
      ) ?? null
    );
  }, [form.cohortSelection, form.gpuName, gpuOptions]);

  const rankedCohorts = useMemo(() => {
    const filtered = cohortUniverse.filter((row) => (cohortFilter === "all" ? true : row.recommendation === cohortFilter));
    const sorted = [...filtered].sort((a, b) => {
      if (cohortSort === "readiness") return b.readinessScore - a.readinessScore;
      if (cohortSort === "opportunity") return b.exploratoryOpportunityScore - a.exploratoryOpportunityScore;
      if (cohortSort === "consumption") return b.exploratoryOpportunityScore - a.exploratoryOpportunityScore;
      if (cohortSort === "inferability") return b.inferability - a.inferability;
      return mode === "exploratory" ? a.exploratoryRank - b.exploratoryRank : a.globalRank - b.globalRank;
    });
    return sorted;
  }, [cohortUniverse, cohortFilter, cohortSort, mode]);

  const compareRows = useMemo(
    () => selectedCompareKeys.map((key) => cohortUniverse.find((row) => row.key === key)).filter((row): row is CohortUniverseEntry => row != null),
    [cohortUniverse, selectedCompareKeys],
  );
  const selectedDetailRow = useMemo(
    () => (detailKey ? cohortUniverse.find((row) => row.key === detailKey) ?? null : null),
    [cohortUniverse, detailKey],
  );

  const filteredPressureTimeline = useMemo(() => {
    if (!result) return [];
    const hours = range === "24h" ? 24 : range === "72h" ? 72 : 24 * 7;
    return filterSeriesByRangeHours(result.visuals.pressureTimeline, hours);
  }, [result, range]);

  const filteredSupplyTimeline = useMemo(() => {
    if (!result) return [];
    const hours = range === "24h" ? 24 : range === "72h" ? 72 : 24 * 7;
    return filterSeriesByRangeHours(result.visuals.supplyTimeline, hours);
  }, [result, range]);

  const isSuppressedRegime =
    result?.currentState.state === "non-inferable" || result?.currentState.state === "churn-dominated";
  const hasSparseCalibration = (result?.visuals.calibration.length ?? 0) < 3;

  function onLegendClick(chart: ChartLegendKey, payload: unknown) {
    const maybePayload = payload as { dataKey?: unknown };
    if (typeof maybePayload?.dataKey !== "string") return;
    const clickedKey = maybePayload.dataKey;
    setLegendSelection((current) => {
      const selected = current[chart];
      if (selected == null) {
        return { ...current, [chart]: [clickedKey] };
      }
      if (selected.includes(clickedKey)) {
        const next = selected.filter((key) => key !== clickedKey);
        return { ...current, [chart]: next.length === 0 ? null : next };
      }
      return { ...current, [chart]: [...selected, clickedKey] };
    });
  }

  function isLegendSeriesVisible(chart: ChartLegendKey, dataKey: string) {
    const selected = legendSelection[chart];
    return selected == null || selected.includes(dataKey);
  }

  function toggleCompare(key: string) {
    setSelectedCompareKeys((current) => {
      if (current.includes(key)) return current.filter((item) => item !== key);
      if (current.length >= 5) return current;
      return [...current, key];
    });
  }

  async function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const body: Record<string, unknown> = {
        gpuName: form.gpuName,
        gpuCount: form.gpuCount,
        assumedPowerWatts: form.assumedPowerWatts,
        assumedHardwareCost: form.assumedHardwareCost,
        electricityCostPerKwh: form.electricityCostPerKwh,
        targetPaybackMonths: form.targetPaybackMonths,
        source: form.source,
        hoursWindow: form.hoursWindow,
      };

      if (form.listingPricePerHour !== "") {
        body.listingPricePerHour = Number(form.listingPricePerHour);
      }

      if (form.cohortSelection !== "combined") {
        const [numRaw, offerType] = form.cohortSelection.split("::");
        const num = Number(numRaw);
        if (Number.isFinite(num)) {
          body.cohortNumGpus = num;
        }
        if (offerType) {
          body.cohortOfferType = offerType;
        }
      }

      const response = await fetch("/api/scoring", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const data = (await response.json()) as ForecastResponse | { error: string; issues?: Array<{ path?: Array<string | number>; message?: string }> };
      if (!response.ok || "error" in data) {
        setResult(null);
        if ("error" in data) {
          const issue = data.issues?.[0];
          const detail =
            issue?.message == null
              ? ""
              : ` (${(issue.path ?? []).join(".") || "payload"}: ${issue.message})`;
          setError(`${data.error}${detail}`);
        } else {
          setError("Scoring failed");
        }
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

  return (
    <main className="mx-auto w-full max-w-7xl p-4 text-sm text-zinc-100 md:p-8">
      <h1 className="mb-4 text-2xl font-semibold">Predictive Probability Engine</h1>

      <section className="mb-4 grid gap-3 rounded border border-zinc-700 bg-zinc-900 p-3 md:grid-cols-3">
        <div className="rounded border border-zinc-700 bg-zinc-950 p-3">
          <p className="text-xs text-zinc-400">Mode</p>
          <div className="mt-2 flex gap-2">
            <button
              type="button"
              onClick={() => setMode("conservative")}
              className={`rounded border px-3 py-1 ${mode === "conservative" ? "border-blue-400 bg-blue-500/20" : "border-zinc-600"}`}
            >
              Conservative
            </button>
            <button
              type="button"
              onClick={() => setMode("exploratory")}
              className={`rounded border px-3 py-1 ${mode === "exploratory" ? "border-emerald-400 bg-emerald-500/20" : "border-zinc-600"}`}
            >
              Exploratory
            </button>
          </div>
          <p className="mt-2 text-xs text-zinc-400">
            {mode === "conservative"
              ? "Conservative preserves suppression/veto as source of truth."
              : "Exploratory shows relative opportunity even when suppressed."}
          </p>
        </div>

        <div className="rounded border border-zinc-700 bg-zinc-950 p-3">
          <p className="text-xs text-zinc-400">Polling & Observability</p>
          <p className="mt-1 text-sm">
            Tiers: high {pollingSummary?.highPriorityTargetMinutes ?? "2-5"}m, general {pollingSummary?.generalTargetMinutes ?? "5-10"}m, long-tail {pollingSummary?.longTailTargetMinutes ?? "10-20"}m
          </p>
          <p className="text-xs text-zinc-400">Use exploratory mode for relative ranking while cohorts mature.</p>
        </div>

        <div className="rounded border border-zinc-700 bg-zinc-950 p-3">
          <p className="text-xs text-zinc-400">Suppression Summary</p>
          <p className="mt-1 text-sm">
            Suppressed {suppressionSummary?.suppressedCount ?? 0} | Near usable {suppressionSummary?.nearUsableCount ?? 0} | Under-sampled {suppressionSummary?.underSampledCount ?? 0}
          </p>
          <p className="text-xs text-zinc-400">
            Identity-constrained {suppressionSummary?.identityConstrainedCount ?? 0} | Churn-heavy {suppressionSummary?.churnHeavyCount ?? 0} | Graduating soon {suppressionSummary?.graduatingSoonCount ?? 0}
          </p>
        </div>
      </section>

      <section className="mb-4 grid gap-3 md:grid-cols-2">
        <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
          <p className="mb-2 text-sm font-semibold">Recommendation Distribution</p>
          <div className="grid grid-cols-5 gap-2 text-xs">
            {(["Avoid", "Watch", "Speculative", "Buy if discounted", "Buy"] as const).map((label) => (
              <div key={label} className="rounded border border-zinc-700 bg-zinc-950 p-2 text-center">
                <p className="text-zinc-400">{label}</p>
                <p className="text-lg font-semibold">{recommendationDistribution[label] ?? 0}</p>
              </div>
            ))}
          </div>
        </div>
        <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
          <p className="mb-2 text-sm font-semibold">Regime Distribution</p>
          <div className="grid grid-cols-4 gap-2 text-xs">
            {["non-inferable", "thin-data", "churn-dominated", "balanced", "tightening", "tight", "oversupplied", "volatile"].map((label) => (
              <div key={label} className="rounded border border-zinc-700 bg-zinc-950 p-2 text-center">
                <p className="text-zinc-400">{label}</p>
                <p className="text-lg font-semibold">{regimeDistribution[label] ?? 0}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="mb-4 rounded border border-zinc-700 bg-zinc-900 p-3">
        <div className="mb-2 flex flex-wrap items-end gap-2">
          <label className="flex flex-col gap-1 text-xs">
            Recommendation Filter
            <select
              className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1 text-sm"
              value={cohortFilter}
              onChange={(event) =>
                setCohortFilter(
                  event.target.value as "all" | "Avoid" | "Watch" | "Speculative" | "Buy if discounted" | "Buy",
                )
              }
            >
              <option value="all">All</option>
              <option value="Avoid">Avoid</option>
              <option value="Watch">Watch</option>
              <option value="Speculative">Speculative</option>
              <option value="Buy if discounted">Buy if discounted</option>
              <option value="Buy">Buy</option>
            </select>
          </label>
          <label className="flex flex-col gap-1 text-xs">
            Sort
            <select
              className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1 text-sm"
              value={cohortSort}
              onChange={(event) =>
                setCohortSort(
                  event.target.value as "rank" | "readiness" | "opportunity" | "consumption" | "inferability",
                )
              }
            >
              <option value="rank">Global Rank</option>
              <option value="readiness">Readiness</option>
              <option value="opportunity">Exploratory Opportunity</option>
              <option value="consumption">Consumption Proxy</option>
              <option value="inferability">Inferability</option>
            </select>
          </label>
          <p className="text-xs text-zinc-400">Select up to 5 cohorts for side-by-side compare.</p>
        </div>
        <div className="max-h-80 overflow-auto">
          <table className="min-w-full text-xs">
            <thead className="text-zinc-400">
              <tr>
                <th className="px-2 py-1 text-left">Compare</th>
                <th className="px-2 py-1 text-left">Rank</th>
                <th className="px-2 py-1 text-left">GPU</th>
                <th className="px-2 py-1 text-left">Config</th>
                <th className="px-2 py-1 text-left">Recommendation</th>
                <th className="px-2 py-1 text-left">Regime</th>
                <th className="px-2 py-1 text-left">Readiness</th>
                <th className="px-2 py-1 text-left">Inferability</th>
                <th className="px-2 py-1 text-left">Confidence</th>
                <th className="px-2 py-1 text-left">Sampling</th>
                <th className="px-2 py-1 text-left">Identity</th>
              </tr>
            </thead>
            <tbody>
              {rankedCohorts.slice(0, 120).map((row) => (
                <tr key={row.key} className="cursor-pointer border-t border-zinc-800 hover:bg-zinc-800/40" onClick={() => setDetailKey(row.key)}>
                  <td className="px-2 py-1">
                    <input
                      type="checkbox"
                      checked={selectedCompareKeys.includes(row.key)}
                      onChange={(event) => {
                        event.stopPropagation();
                        toggleCompare(row.key);
                      }}
                    />
                  </td>
                  <td className="px-2 py-1">{mode === "exploratory" ? row.exploratoryRank : row.globalRank}</td>
                  <td className="px-2 py-1">{row.gpuName}</td>
                  <td className="px-2 py-1">
                    {row.cohortNumGpus ?? "combined"}x / {row.cohortOfferType ?? "combined"}
                  </td>
                  <td className="px-2 py-1">{row.recommendation}</td>
                  <td className="px-2 py-1">{row.state}</td>
                  <td className="px-2 py-1">{row.readinessScore.toFixed(1)} ({row.readinessBand})</td>
                  <td className="px-2 py-1">{row.inferability.toFixed(1)}</td>
                  <td className="px-2 py-1">{row.confidence.toFixed(1)}</td>
                  <td className="px-2 py-1">{row.samplingQualityScore.toFixed(1)}</td>
                  <td className="px-2 py-1">{row.identityQualityScore.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {compareRows.length > 0 ? (
        <section className="mb-4 rounded border border-zinc-700 bg-zinc-900 p-3">
          <p className="mb-2 text-sm font-semibold">Compare ({compareRows.length}/5)</p>
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead className="text-zinc-400">
                <tr>
                  <th className="px-2 py-1 text-left">GPU</th>
                  <th className="px-2 py-1 text-left">num GPUs</th>
                  <th className="px-2 py-1 text-left">Offer type</th>
                  <th className="px-2 py-1 text-left">Recommendation</th>
                  <th className="px-2 py-1 text-left">Regime</th>
                  <th className="px-2 py-1 text-left">Readiness</th>
                  <th className="px-2 py-1 text-left">Inferability</th>
                  <th className="px-2 py-1 text-left">Confidence</th>
                  <th className="px-2 py-1 text-left">Sampling</th>
                  <th className="px-2 py-1 text-left">Identity</th>
                  <th className="px-2 py-1 text-left">Pressure</th>
                  <th className="px-2 py-1 text-left">Churn penalty</th>
                  <th className="px-2 py-1 text-left">Exploratory rank</th>
                  <th className="px-2 py-1 text-left">Upgrade guidance</th>
                </tr>
              </thead>
              <tbody>
                {compareRows.map((row) => (
                  <tr key={row.key} className="border-t border-zinc-800">
                    <td className="px-2 py-1">{row.gpuName}</td>
                    <td className="px-2 py-1">{row.cohortNumGpus ?? "combined"}</td>
                    <td className="px-2 py-1">{row.cohortOfferType ?? "combined"}</td>
                    <td className="px-2 py-1">{row.recommendation}</td>
                    <td className="px-2 py-1">{row.state}</td>
                    <td className="px-2 py-1">{row.readinessScore.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.inferability.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.confidence.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.samplingQualityScore.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.identityQualityScore.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.pressure.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.churnScore.toFixed(1)}</td>
                    <td className="px-2 py-1">{row.exploratoryRank}</td>
                    <td className="px-2 py-1">{row.upgradeGuidance.slice(0, 2).join(" ") || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {selectedDetailRow ? (
        <section className="mb-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-zinc-900">
          <h2 className="text-base font-semibold">
            Why this result? {selectedDetailRow.gpuName} {selectedDetailRow.cohortNumGpus ?? "combined"}x / {selectedDetailRow.cohortOfferType ?? "combined"}
          </h2>
          <p className="text-sm">
            Recommendation: {selectedDetailRow.recommendation} | Regime: {selectedDetailRow.state} | Readiness {selectedDetailRow.readinessScore.toFixed(1)} ({selectedDetailRow.readinessBand})
          </p>
          <p className="text-sm">
            Supporting: pressure {selectedDetailRow.pressure.toFixed(1)}, inferability {selectedDetailRow.inferability.toFixed(1)}, confidence {selectedDetailRow.confidence.toFixed(1)}
          </p>
          <p className="text-sm">
            Limits: sampling {selectedDetailRow.samplingQualityScore.toFixed(1)}, identity {selectedDetailRow.identityQualityScore.toFixed(1)}, churn {selectedDetailRow.churnScore.toFixed(1)}
          </p>
          <p className="text-sm">What would change this? {selectedDetailRow.upgradeGuidance.slice(0, 3).join(" ") || "Maintain current metrics."}</p>
          <p className="text-sm">Downgrade risks: {selectedDetailRow.downgradeRiskFactors.slice(0, 3).join(" ") || "No immediate downgrade trigger."}</p>
        </section>
      ) : null}

      <form onSubmit={onSubmit} className="mb-6 grid gap-3 rounded border border-zinc-700 bg-zinc-900 p-4 md:grid-cols-4">
        <label className="flex flex-col gap-1">
          GPU
          <select
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.gpuName}
            onChange={(event) => {
              const gpuName = event.target.value;
              const option = gpuOptions.find((candidate) => candidate.gpuName === gpuName);
              setForm((current) => ({
                ...current,
                gpuName,
                cohortSelection: "combined",
                source: option?.source ?? current.source,
                gpuCount: option?.defaults.gpuCount ?? current.gpuCount,
                assumedPowerWatts: option?.defaults.assumedPowerWatts ?? current.assumedPowerWatts,
                assumedHardwareCost: option?.defaults.assumedHardwareCost ?? current.assumedHardwareCost,
                electricityCostPerKwh: option?.defaults.electricityCostPerKwh ?? current.electricityCostPerKwh,
                targetPaybackMonths: option?.defaults.targetPaybackMonths ?? current.targetPaybackMonths,
                hoursWindow: option?.defaults.hoursWindow ?? current.hoursWindow,
              }));
            }}
            disabled={metaLoading || gpuNames.length === 0}
          >
            {gpuNames.map((gpuName) => (
              <option key={gpuName} value={gpuName}>
                {gpuName}
              </option>
            ))}
          </select>
        </label>

        <label className="flex flex-col gap-1">
          Cohort View
          <select
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.cohortSelection}
            onChange={(event) => setForm((current) => ({ ...current, cohortSelection: event.target.value }))}
          >
            <option value="combined">Family combined (all configs)</option>
            {cohortOptions.map((option) => {
              if (option.cohortNumGpus == null || option.cohortOfferType == null) return null;
              const key = `${option.cohortNumGpus}::${option.cohortOfferType}`;
              return (
                <option key={`${option.gpuName}-${key}`} value={key}>
                  Exact: {option.cohortNumGpus}x / {option.cohortOfferType}
                </option>
              );
            })}
          </select>
        </label>

        <label className="flex flex-col gap-1">
          Listing Price ($/h)
          <input
            type="number"
            step="0.001"
            className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
            value={form.listingPricePerHour}
            onChange={(event) =>
              setForm((current) => ({
                ...current,
                listingPricePerHour: event.target.value === "" ? "" : Number(event.target.value),
              }))
            }
          />
        </label>

        <div className="flex items-end gap-2">
          <button
            type="button"
            onClick={() => setShowAdvanced((value) => !value)}
            className="rounded border border-zinc-600 px-3 py-1"
          >
            {showAdvanced ? "Hide advanced" : "Show advanced"}
          </button>
          <button
            type="submit"
            disabled={loading || metaLoading || !form.gpuName}
            className="rounded bg-blue-600 px-4 py-1 font-medium text-white disabled:opacity-60"
          >
            {loading ? "Forecasting..." : "Run Forecast"}
          </button>
        </div>

        {showAdvanced ? (
          <>
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
              Power (W)
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
              Window (hours)
              <input
                type="number"
                className="rounded border border-zinc-600 bg-zinc-950 px-2 py-1"
                value={form.hoursWindow}
                onChange={(event) => updateNumber("hoursWindow", event.target.value)}
              />
            </label>
          </>
        ) : null}

        <div className="col-span-full text-xs text-zinc-300">
          {selectedOption ? (
            <span>
              Latest exact cohort snapshot: pressure {selectedOption.latestPressure?.toFixed(1) ?? "n/a"} | median {selectedOption.latestMedianPrice == null ? "n/a" : `$${selectedOption.latestMedianPrice.toFixed(3)}/h`} | machines {selectedOption.latestUniqueMachines ?? "n/a"}
            </span>
          ) : (
            <span>Using family-combined cohort context for stability and shrinkage.</span>
          )}
        </div>
      </form>

      {error ? <section className="mb-4 rounded border border-red-500 bg-red-950/40 p-3 text-red-200">{error}</section> : null}

      {result ? (
        <>
          {isSuppressedRegime ? (
            <section className="mb-4 rounded border border-amber-500 bg-amber-950/40 p-3 text-amber-100">
              <p className="font-semibold">
                {result.currentState.state === "non-inferable"
                  ? "Insufficient signal: this regime is non-inferable and outputs are intentionally suppressed."
                  : "Churn-dominated regime: movement exists, but durable removal signal is weak."}
              </p>
              <p className="text-sm">
                Inferability {result.currentState.inferabilityScore.toFixed(1)} | Signal strength {result.currentState.signalStrengthScore.toFixed(1)} | Identity quality {result.currentState.identityQualityScore.toFixed(1)} | time depth {result.currentState.timeDepthScore.toFixed(1)} | cross-section depth {result.currentState.crossSectionDepthScore.toFixed(1)}
              </p>
            </section>
          ) : null}

          <section className="mb-6 grid gap-3 rounded border border-zinc-200 bg-zinc-50 p-4 text-zinc-900 md:grid-cols-5">
            <div className="md:col-span-2 rounded border border-zinc-200 bg-white p-3">
              <h2 className="text-lg font-semibold">{result.recommendation}</h2>
              <p className="mt-1 text-sm text-zinc-600">{result.recommendationReasonPrimary}</p>
              <p className="text-sm text-zinc-600">{result.recommendationReasonSecondary}</p>
              <p className="mt-1 text-xs text-zinc-500">{result.recommendationConfidenceNote}</p>
            </div>
            <div className="rounded border border-zinc-200 bg-white p-3">
              <p className="text-xs text-zinc-500">Expected Utilization</p>
              <p className="text-xl font-semibold">{pct(result.utilization.expected)}</p>
              <p className="text-xs text-zinc-500">
                {pct(result.utilization.low)} - {pct(result.utilization.high)}
              </p>
            </div>
            <div className="rounded border border-zinc-200 bg-white p-3">
              <p className="text-xs text-zinc-500">Expected Payback</p>
              <p className="text-xl font-semibold">
                {result.economics.expectedPaybackMonths == null ? "n/a" : `${result.economics.expectedPaybackMonths.toFixed(1)}m`}
              </p>
              <p className="text-xs text-zinc-500">
                {result.economics.expectedPaybackMonthsLow == null ? "n/a" : `${result.economics.expectedPaybackMonthsLow.toFixed(1)}m`} - {result.economics.expectedPaybackMonthsHigh == null ? "n/a" : `${result.economics.expectedPaybackMonthsHigh.toFixed(1)}m`}
              </p>
            </div>
            <div className="rounded border border-zinc-200 bg-white p-3">
              <p className="text-xs text-zinc-500">Readiness / Payback</p>
              <p className="text-xl font-semibold">{pct(result.economics.pPaybackWithinTarget)}</p>
              <p className="text-xs text-zinc-500">
                Readiness {result.readiness.readinessScore.toFixed(1)} ({result.readiness.readinessBand}) | Inferability {result.currentState.inferabilityScore.toFixed(1)}
              </p>
            </div>
          </section>

          <section className="mb-6 grid gap-3 md:grid-cols-4">
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <p className="text-xs text-zinc-400">P(Tight 24h)</p>
              <p className="text-lg font-semibold">{pct(result.forecastProbabilities.pTight24h)}</p>
            </div>
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <p className="text-xs text-zinc-400">P(Tight 72h)</p>
              <p className="text-lg font-semibold">{pct(result.forecastProbabilities.pTight72h)}</p>
            </div>
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <p className="text-xs text-zinc-400">P(Tight 7d)</p>
              <p className="text-lg font-semibold">{pct(result.forecastProbabilities.pTight7d)}</p>
            </div>
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <p className="text-xs text-zinc-400">State / Trust</p>
              <p className="text-lg font-semibold">{result.currentState.state}</p>
              <p className="text-xs text-zinc-400">
                conf {result.currentState.confidenceScore.toFixed(1)} | infer {result.currentState.inferabilityScore.toFixed(1)}
              </p>
            </div>
          </section>

          <section className="mb-6 grid gap-3 md:grid-cols-3">
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-1 text-sm font-semibold">Observation Quality</h3>
              <p className="text-xs text-zinc-400">
                Freshness quality {result.observationQuality.dataFreshnessQuality} | density {result.observationQuality.cohortObservationDensityScore.toFixed(1)}
              </p>
              <p className="text-xs text-zinc-400">
                obs {result.observationQuality.observationCount} | per offer {result.observationQuality.observationsPerOffer.toFixed(2)}
              </p>
              <p className="text-xs text-zinc-400">
                median gap {result.observationQuality.medianPollGapMinutes.toFixed(1)}m | max gap {result.observationQuality.maxPollGapMinutes.toFixed(1)}m
              </p>
              <p className="text-xs text-zinc-400">
                sampling {result.observationQuality.samplingQualityScore.toFixed(1)} | lifecycle {result.observationQuality.lifecycleObservabilityScore.toFixed(1)}
              </p>
            </div>
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-1 text-sm font-semibold">Why This Result?</h3>
              <p className="text-xs text-zinc-400">{result.displayRecommendationReason}</p>
              <p className="mt-1 text-xs text-zinc-400">
                Decomposition: market {(result.inferabilityDecomposition.marketAmbiguity * 100).toFixed(0)}% | sampling {(result.inferabilityDecomposition.poorSampling * 100).toFixed(0)}% | identity {(result.inferabilityDecomposition.weakIdentity * 100).toFixed(0)}%
              </p>
              <p className="mt-1 text-xs text-zinc-400">{result.suppressionReasons.join(" ")}</p>
              <p className="text-xs text-zinc-400">{result.samplingReasons.join(" ")}</p>
            </div>
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-1 text-sm font-semibold">What Would Change This?</h3>
              <p className="text-xs text-zinc-400">Nearest upgrade: {result.nearestUpgrade ?? "-"}</p>
              <p className="text-xs text-zinc-400">Nearest downgrade: {result.nearestDowngrade ?? "-"}</p>
              <p className="mt-1 text-xs text-zinc-400">{result.upgradeGuidance.slice(0, 3).join(" ") || "No immediate upgrade blocker."}</p>
              <p className="mt-1 text-xs text-zinc-400">{result.downgradeRiskFactors.slice(0, 3).join(" ") || "No immediate downgrade trigger."}</p>
            </div>
          </section>

          <section className="mb-3 flex gap-2">
            {(["24h", "72h", "7d"] as const).map((value) => (
              <button
                key={value}
                type="button"
                className={`rounded border px-3 py-1 ${range === value ? "border-blue-400 bg-blue-500/20" : "border-zinc-600"}`}
                onClick={() => setRange(value)}
              >
                {value}
              </button>
            ))}
          </section>

          <section className="mb-6 grid gap-4 md:grid-cols-2">
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Cohort Pressure Timeline</h3>
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <LineChart data={filteredPressureTimeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="bucketStartUtc" hide />
                    <YAxis domain={[0, 100]} />
                    <Tooltip />
                    <ReferenceArea y1={0} y2={40} fill="#6b1f1f" fillOpacity={0.15} />
                    <ReferenceArea y1={40} y2={70} fill="#786f29" fillOpacity={0.12} />
                    <ReferenceArea y1={70} y2={100} fill="#18543b" fillOpacity={0.12} />
                    <Area type="monotone" dataKey="pressureHigh" stroke="none" fill="#60a5fa" fillOpacity={0.1} />
                    <Area type="monotone" dataKey="pressureLow" stroke="none" fill="#111827" fillOpacity={1} />
                    <Line type="monotone" dataKey="pressure" stroke="#60a5fa" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Visible Supply + Unique Machines</h3>
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <LineChart data={filteredSupplyTimeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="bucketStartUtc" hide />
                    <YAxis />
                    <Tooltip />
                    <Legend onClick={(payload) => onLegendClick("supply", payload)} />
                    <Line type="monotone" dataKey="totalOffers" stroke="#f97316" dot={false} hide={!isLegendSeriesVisible("supply", "totalOffers")} />
                    <Line type="monotone" dataKey="uniqueMachines" stroke="#22c55e" dot={false} hide={!isLegendSeriesVisible("supply", "uniqueMachines")} />
                    <Line type="monotone" dataKey="newOffers" stroke="#60a5fa" dot={false} hide={!isLegendSeriesVisible("supply", "newOffers")} />
                    <Line type="monotone" dataKey="disappearedOffers" stroke="#f43f5e" dot={false} hide={!isLegendSeriesVisible("supply", "disappearedOffers")} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Offer Lifecycle / Survival</h3>
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <BarChart data={result.visuals.offerSurvival}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="durationHoursBucket" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="count" stackId="a" fill="#38bdf8" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Price Position vs Consumption</h3>
              {isSuppressedRegime ? (
                <p className="mb-2 text-xs text-amber-300">
                  Curve flattened toward neutral due to insufficient inferable signal.
                </p>
              ) : null}
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <LineChart data={result.visuals.pricePositionCurve}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="relativePricePosition" />
                    <YAxis domain={[0, 1]} />
                    <Tooltip />
                    <Legend onClick={(payload) => onLegendClick("priceCurve", payload)} />
                    <Area type="monotone" dataKey="p24hHigh" stroke="none" fill="#60a5fa" fillOpacity={0.12} />
                    <Area type="monotone" dataKey="p24hLow" stroke="none" fill="#111827" fillOpacity={1} />
                    <Line type="monotone" dataKey="p12h" stroke="#22c55e" dot={false} hide={!isLegendSeriesVisible("priceCurve", "p12h")} />
                    <Line type="monotone" dataKey="p24h" stroke="#60a5fa" dot={false} hide={!isLegendSeriesVisible("priceCurve", "p24h")} />
                    <Line type="monotone" dataKey="p72h" stroke="#f97316" dot={false} hide={!isLegendSeriesVisible("priceCurve", "p72h")} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Config Comparison (1x/2x/4x/8x+)</h3>
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <LineChart data={result.visuals.configComparison}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="numGpus" />
                    <YAxis yAxisId="left" domain={[0, 100]} />
                    <YAxis yAxisId="right" orientation="right" domain={[0, 1]} />
                    <Tooltip />
                    <Legend onClick={(payload) => onLegendClick("config", payload)} />
                    <Line yAxisId="left" type="monotone" dataKey="pressure" stroke="#60a5fa" dot hide={!isLegendSeriesVisible("config", "pressure")} />
                    <Line yAxisId="right" type="monotone" dataKey="hazard" stroke="#22c55e" dot hide={!isLegendSeriesVisible("config", "hazard")} />
                    <Line yAxisId="left" type="monotone" dataKey="confidence" stroke="#f59e0b" dot hide={!isLegendSeriesVisible("config", "confidence")} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Market Map (Utilization vs Payback)</h3>
              {isSuppressedRegime ? (
                <p className="mb-2 text-xs text-amber-300">
                  Map is intentionally conservative in suppressed regimes; treat as risk guardrails, not upside ranking.
                </p>
              ) : null}
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <ScatterChart>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="expectedUtilization" name="Utilization" domain={[0, 1]} />
                    <YAxis dataKey="expectedPayback" name="Payback (months)" />
                    <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                    <Scatter data={result.visuals.marketMap} fill="#60a5fa" />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3 md:col-span-2">
              <h3 className="mb-2 text-sm font-semibold">Calibration Chart (Predicted vs Realized)</h3>
              {hasSparseCalibration ? (
                <p className="mb-2 text-xs text-amber-300">
                  Insufficient calibration samples for stable bucket interpretation.
                </p>
              ) : null}
              <div className="h-64 w-full">
                <ResponsiveContainer>
                  <LineChart data={result.visuals.calibration}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="bucket" />
                    <YAxis domain={[0, 1]} />
                    <Tooltip />
                    <Legend onClick={(payload) => onLegendClick("calibration", payload)} />
                    <Line type="monotone" dataKey="avgPredicted" stroke="#60a5fa" dot={false} hide={!isLegendSeriesVisible("calibration", "avgPredicted")} />
                    <Line type="monotone" dataKey="realizedRate" stroke="#22c55e" dot={false} hide={!isLegendSeriesVisible("calibration", "realizedRate")} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </section>

          <section className="mb-6 grid gap-3 md:grid-cols-2">
            <div className="rounded border border-zinc-200 bg-zinc-50 p-3 text-zinc-900">
              <h3 className="mb-2 text-base font-semibold">Observed vs Inferred vs Forecasted</h3>
              <p className="text-xs text-zinc-500">Observed</p>
              <ul className="mb-2 list-disc pl-5">
                {result.explanation.observed?.map((item) => <li key={item}>{item}</li>)}
              </ul>
              <p className="text-xs text-zinc-500">Inferred</p>
              <ul className="mb-2 list-disc pl-5">
                {result.explanation.inferred?.map((item) => <li key={item}>{item}</li>)}
              </ul>
              <p className="text-xs text-zinc-500">Forecasted</p>
              <ul className="list-disc pl-5">
                {result.explanation.forecasted?.map((item) => <li key={item}>{item}</li>)}
              </ul>
            </div>

            <div className="rounded border border-zinc-200 bg-zinc-50 p-3 text-zinc-900">
              <h3 className="mb-2 text-base font-semibold">Forecast Panel</h3>
              <p>Price direction (24h): up {pct(result.forecastProbabilities.pPriceUp24h)}, flat {pct(result.forecastProbabilities.pPriceFlat24h)}, down {pct(result.forecastProbabilities.pPriceDown24h)}</p>
              <p>Exact vs family pressure: {result.currentState.pressure.toFixed(1)} vs {result.familyBaseline.pressure.toFixed(1)}</p>
              <p>Machine depth score: {result.familyBaseline.machineDepth.toFixed(1)}</p>
              <p>Concentration top3: {pct(result.exactCohort.machineConcentrationShareTop3)}</p>
              <p>Noise: {result.currentState.noiseScore.toFixed(1)} | Data depth: {result.currentState.dataDepthScore.toFixed(1)} (time {result.currentState.timeDepthScore.toFixed(1)} / cross-section {result.currentState.crossSectionDepthScore.toFixed(1)})</p>
              <p>Movement: {result.currentState.movementScore.toFixed(1)} | Churn: {result.currentState.churnScore.toFixed(1)} | Signal: {result.currentState.signalStrengthScore.toFixed(1)} | Inferability: {result.currentState.inferabilityScore.toFixed(1)}</p>
              <p>Consumption p(24h): raw {pct(result.forecastProbabilities.pOfferConsumedWithin24hRaw)} | calibrated {pct(result.forecastProbabilities.pOfferConsumedWithin24hCalibrated)} | suppressed output {pct(result.forecastProbabilities.pOfferConsumedWithin24h)}</p>
              <p>Revenue range: {dollars(result.economics.expectedDailyRevenueLow)} - {dollars(result.economics.expectedDailyRevenueHigh)}</p>
              <p>Margin range: {dollars(result.economics.expectedDailyMarginLow)} - {dollars(result.economics.expectedDailyMarginHigh)}</p>
            </div>
          </section>

          <section className="mb-6 grid gap-3 md:grid-cols-2">
            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Latest Offers (Drill-down)</h3>
              <div className="max-h-64 overflow-auto">
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="text-zinc-400">
                      <th className="px-2 py-1 text-left">Offer</th>
                      <th className="px-2 py-1 text-left">Machine</th>
                      <th className="px-2 py-1 text-left">Host</th>
                      <th className="px-2 py-1 text-left">Price</th>
                      <th className="px-2 py-1 text-left">Reliability</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.drilldowns.latestOffers.slice(0, 20).map((offer) => (
                      <tr key={`${offer.offerId}-${offer.machineId ?? "na"}`}>
                        <td className="px-2 py-1">{offer.offerId}</td>
                        <td className="px-2 py-1">{offer.machineId ?? "-"}</td>
                        <td className="px-2 py-1">{offer.hostId ?? "-"}</td>
                        <td className="px-2 py-1">{offer.pricePerHour == null ? "-" : `$${offer.pricePerHour.toFixed(3)}`}</td>
                        <td className="px-2 py-1">{offer.reliabilityScore?.toFixed(3) ?? "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded border border-zinc-700 bg-zinc-900 p-3">
              <h3 className="mb-2 text-sm font-semibold">Machine Concentration</h3>
              <div className="max-h-64 overflow-auto">
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="text-zinc-400">
                      <th className="px-2 py-1 text-left">Machine</th>
                      <th className="px-2 py-1 text-left">Offers</th>
                      <th className="px-2 py-1 text-left">Share</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.drilldowns.machineConcentration.map((row) => (
                      <tr key={`machine-${row.machineId ?? "na"}`}>
                        <td className="px-2 py-1">{row.machineId ?? "unknown"}</td>
                        <td className="px-2 py-1">{row.offers}</td>
                        <td className="px-2 py-1">{pct(row.share)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <section className="mb-6 rounded border border-zinc-700 bg-zinc-900 p-3">
            <h3 className="mb-2 text-sm font-semibold">Realized Label Quality</h3>
            <div className="overflow-x-auto">
              <table className="min-w-full text-xs">
                <thead>
                  <tr className="text-zinc-400">
                    <th className="px-2 py-1 text-left">Horizon</th>
                    <th className="px-2 py-1 text-left">Quality</th>
                    <th className="px-2 py-1 text-left">Count</th>
                  </tr>
                </thead>
                <tbody>
                  {result.drilldowns.labelQualitySummary.map((row) => (
                    <tr key={`${row.horizonHours}-${row.quality}`} className="border-t border-zinc-800">
                      <td className="px-2 py-1">{row.horizonHours}h</td>
                      <td className="px-2 py-1">{row.quality}</td>
                      <td className="px-2 py-1">{row.count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      <section className="rounded border border-zinc-200 bg-zinc-50 p-4 text-zinc-900">
        <h2 className="mb-2 text-lg font-semibold">Recent Scenario Forecasts</h2>
        {recentScenarios.length === 0 ? (
          <p>No scenarios saved yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-zinc-200 text-sm">
              <thead className="bg-zinc-100 text-left text-zinc-700">
                <tr>
                  <th className="px-3 py-2">GPU</th>
                  <th className="px-3 py-2">Count</th>
                  <th className="px-3 py-2">Target</th>
                  <th className="px-3 py-2">Utilization</th>
                  <th className="px-3 py-2">Payback</th>
                  <th className="px-3 py-2">Confidence</th>
                  <th className="px-3 py-2">Recommendation</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-200">
                {recentScenarios.map((scenario) => (
                  <tr key={scenario.id}>
                    <td className="px-3 py-2">{scenario.gpuName}</td>
                    <td className="px-3 py-2">{scenario.gpuCount}</td>
                    <td className="px-3 py-2">{scenario.targetPaybackMonths}m</td>
                    <td className="px-3 py-2">{pct(scenario.expectedUtilization)}</td>
                    <td className="px-3 py-2">{scenario.expectedPaybackMonths == null ? "n/a" : `${scenario.expectedPaybackMonths.toFixed(1)}m`}</td>
                    <td className="px-3 py-2">{scenario.confidenceScore.toFixed(1)}</td>
                    <td className="px-3 py-2">{scenario.latestScore?.recommendation ?? "-"}</td>
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

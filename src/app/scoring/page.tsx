import { calculateScenarioScore } from "@/lib/scoring/score";

export default function ScoringPage() {
  const example = calculateScenarioScore({
    demandScore: 78,
    priceStrengthScore: 65,
    competitionScore: 58,
    efficiencyScore: 72,
  });

  return (
    <main className="mx-auto w-full max-w-3xl p-6 md:p-10">
      <h1 className="mb-4 text-2xl font-semibold">Scenario Scoring</h1>
      <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm">
        <p className="mb-2">Weighted score model (40/20/20/20) is wired.</p>
        <p>Example overall score: {example.overallScore}</p>
        <p>Recommendation: {example.recommendation}</p>
      </div>
    </main>
  );
}

import type { ScoreInput, ScoreResult } from "@/types/market";

function clamp(value: number, min = 0, max = 100): number {
  return Math.max(min, Math.min(max, value));
}

export function calculateScenarioScore(input: ScoreInput): ScoreResult {
  const demandScore = clamp(input.demandScore);
  const priceStrengthScore = clamp(input.priceStrengthScore);
  const competitionScore = clamp(input.competitionScore);
  const efficiencyScore = clamp(input.efficiencyScore);

  const overallScore =
    demandScore * 0.4 +
    priceStrengthScore * 0.2 +
    competitionScore * 0.2 +
    efficiencyScore * 0.2;

  let recommendation: ScoreResult["recommendation"] = "Avoid";
  if (overallScore >= 75) {
    recommendation = "Buy";
  } else if (overallScore >= 55) {
    recommendation = "Watch";
  }

  return {
    overallScore: Number(overallScore.toFixed(2)),
    recommendation,
  };
}

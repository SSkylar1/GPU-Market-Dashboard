import { clamp } from "@/lib/metrics/intelligence";

export type LegacyScoreInput = {
  demandScore: number;
  priceStrengthScore: number;
  competitionScore: number;
  efficiencyScore: number;
};

export type LegacyScoreResult = {
  overallScore: number;
  recommendation: "Buy" | "Watch" | "Avoid";
};

export function calculateScenarioScore(input: LegacyScoreInput): LegacyScoreResult {
  const demandScore = clamp(input.demandScore, 0, 100);
  const priceStrengthScore = clamp(input.priceStrengthScore, 0, 100);
  const competitionScore = clamp(input.competitionScore, 0, 100);
  const efficiencyScore = clamp(input.efficiencyScore, 0, 100);

  const overallScore =
    demandScore * 0.35 +
    priceStrengthScore * 0.2 +
    competitionScore * 0.2 +
    efficiencyScore * 0.25;

  let recommendation: LegacyScoreResult["recommendation"] = "Avoid";
  if (overallScore >= 72) recommendation = "Buy";
  else if (overallScore >= 52) recommendation = "Watch";

  return {
    overallScore: Number(overallScore.toFixed(2)),
    recommendation,
  };
}

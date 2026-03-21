import type { PriceBands } from "@/types/market";

export function getRecommendedPriceBands(comparablePrices: number[]): PriceBands {
  const valid = comparablePrices.filter((value) => Number.isFinite(value) && value > 0);

  if (valid.length === 0) {
    return { aggressive: 0, target: 0, premium: 0 };
  }

  valid.sort((a, b) => a - b);

  const min = valid[0];
  const max = valid[valid.length - 1];
  const target = valid[Math.floor(valid.length / 2)];
  const spread = Math.max(max - min, target * 0.1);

  const aggressive = Math.max(min, target - spread * 0.35);
  const premium = Math.min(max + spread * 0.2, target + spread * 0.5);

  return {
    aggressive: Number(aggressive.toFixed(4)),
    target: Number(target.toFixed(4)),
    premium: Number(premium.toFixed(4)),
  };
}

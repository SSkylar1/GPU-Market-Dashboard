export type GpuRollupView = {
  gpuName: string;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  impliedUtilization: number;
  medianPrice: number | null;
  minPrice: number | null;
  p90Price: number | null;
};

export type GpuTrendPoint = {
  source: string;
  bucketStartUtc: string;
  snapshotCount: number;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  impliedUtilization: number;
  availabilityRatio: number;
  minPrice: number | null;
  medianPrice: number | null;
  p90Price: number | null;
};

export type PriceBands = {
  aggressive: number;
  target: number;
  premium: number;
};

export type ScoreInput = {
  demandScore: number;
  priceStrengthScore: number;
  competitionScore: number;
  efficiencyScore: number;
};

export type ScoreResult = {
  overallScore: number;
  recommendation: "Buy" | "Watch" | "Avoid";
};

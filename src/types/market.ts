export type GpuRollupView = {
  gpuName: string;
  numGpus?: number;
  offerType?: string;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  impliedUtilization: number;
  availableShare?: number;
  unavailableShare?: number;
  activeLeaseShare?: number;
  leaseSignalShare?: number;
  observedRentedShare?: number;
  distinctHostCount?: number;
  distinctMachineCount?: number;
  latestDisappearedOffers?: number;
  disappearedOffers24h?: number;
  latestNewOffers?: number;
  newOffers24h?: number;
  medianPrice: number | null;
  minPrice: number | null;
  p10Price?: number | null;
  p90Price: number | null;
  marketPressureScore?: number | null;
  regime?: "tight" | "balanced" | "oversupplied";
};

export type GpuTrendPoint = {
  source: string;
  gpuName?: string;
  numGpus?: number;
  offerType?: string;
  bucketStartUtc: string;
  snapshotCount: number;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  impliedUtilization: number;
  availableShare?: number;
  unavailableShare?: number;
  activeLeaseShare?: number;
  leaseSignalShare?: number;
  observedRentedShare?: number;
  newOfferCount?: number;
  disappearedOfferCount?: number;
  availabilityRatio: number;
  newOfferRate?: number | null;
  disappearedRate?: number | null;
  netSupplyChange?: number | null;
  marketPressureScore?: number | null;
  lowBandDisappearedRate?: number | null;
  midBandDisappearedRate?: number | null;
  highBandDisappearedRate?: number | null;
  minPrice: number | null;
  p10Price?: number | null;
  medianPrice: number | null;
  p90Price: number | null;
};

export type HostMachineBreakdown = {
  hostId: number | null;
  machineId: number | null;
  totalOffers: number;
  rentableOffers: number;
  rentedOffers: number;
  medianPrice: number | null;
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

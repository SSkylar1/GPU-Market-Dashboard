export const CONSUMPTION_HORIZONS = [12, 24, 72] as const;

export type ConsumptionHorizon = (typeof CONSUMPTION_HORIZONS)[number];

export type ConsumptionEventLabel = {
  timeToReappearanceBuckets: number | null;
  timeToReappearanceHours: number | null;
  consumedWithin12h: boolean | null;
  consumedWithin24h: boolean | null;
  consumedWithin72h: boolean | null;
  censoredWithin12h: boolean;
  censoredWithin24h: boolean;
  censoredWithin72h: boolean;
};

export function bucketsForHours(hours: number, bucketHours: number): number {
  return Math.max(1, Math.ceil(hours / Math.max(bucketHours, 1e-9)));
}

export function buildConsumptionEventLabel(input: {
  timeToReappearanceBuckets: number | null;
  sourceBucketHours: number;
  futureHoursAvailable: number;
}): ConsumptionEventLabel {
  const timeToReappearanceHours =
    input.timeToReappearanceBuckets == null ? null : input.timeToReappearanceBuckets * input.sourceBucketHours;

  const classify = (horizonHours: ConsumptionHorizon): { consumed: boolean | null; censored: boolean } => {
    if (input.futureHoursAvailable + 1e-9 < horizonHours) {
      return { consumed: null, censored: true };
    }
    const horizonBuckets = bucketsForHours(horizonHours, input.sourceBucketHours);
    const consumed =
      input.timeToReappearanceBuckets == null || input.timeToReappearanceBuckets > horizonBuckets;
    return { consumed, censored: false };
  };

  const h12 = classify(12);
  const h24 = classify(24);
  const h72 = classify(72);

  return {
    timeToReappearanceBuckets: input.timeToReappearanceBuckets,
    timeToReappearanceHours,
    consumedWithin12h: h12.consumed,
    consumedWithin24h: h24.consumed,
    consumedWithin72h: h72.consumed,
    censoredWithin12h: h12.censored,
    censoredWithin24h: h24.censored,
    censoredWithin72h: h72.censored,
  };
}

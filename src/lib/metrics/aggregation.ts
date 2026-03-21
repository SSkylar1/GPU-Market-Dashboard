export type Percentile = 0.5 | 0.9;

export type OfferLike = {
  rentable: boolean;
  rented: boolean;
  pricePerHour: number | null;
};

export function percentile(sortedValues: number[], p: Percentile): number | null {
  if (sortedValues.length === 0) return null;
  const idx = Math.ceil(p * sortedValues.length) - 1;
  const safeIndex = Math.min(sortedValues.length - 1, Math.max(0, idx));
  return sortedValues[safeIndex];
}

export function floorToUtcHalfHour(value: Date): Date {
  const utcMinutes = value.getUTCMinutes();
  const flooredMinutes = utcMinutes < 30 ? 0 : 30;
  return new Date(
    Date.UTC(
      value.getUTCFullYear(),
      value.getUTCMonth(),
      value.getUTCDate(),
      value.getUTCHours(),
      flooredMinutes,
      0,
      0,
    ),
  );
}

export function summarizeOffers(offers: OfferLike[]) {
  const totalOffers = offers.length;
  const rentableOffers = offers.filter((offer) => offer.rentable).length;
  const rentedOffers = offers.filter((offer) => offer.rented).length;
  const impliedUtilization = totalOffers === 0 ? 0 : 1 - rentableOffers / totalOffers;
  const availabilityRatio = totalOffers === 0 ? 0 : rentableOffers / totalOffers;

  const prices = offers
    .map((offer) => offer.pricePerHour)
    .filter((value): value is number => value !== null)
    .sort((a, b) => a - b);

  return {
    totalOffers,
    rentableOffers,
    rentedOffers,
    impliedUtilization,
    availabilityRatio,
    minPrice: prices.length > 0 ? prices[0] : null,
    medianPrice: percentile(prices, 0.5),
    p90Price: percentile(prices, 0.9),
    prices,
  };
}

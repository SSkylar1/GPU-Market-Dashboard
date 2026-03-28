export type TimeSeriesPoint = {
  bucketStartUtc: string;
  [key: string]: number | string;
};

export function filterSeriesByRangeHours<T extends TimeSeriesPoint>(
  series: T[],
  rangeHours: number,
): T[] {
  if (series.length === 0) return [];
  const end = new Date(series[series.length - 1].bucketStartUtc).getTime();
  const minTs = end - rangeHours * 3600 * 1000;
  return series.filter((point) => new Date(point.bucketStartUtc).getTime() >= minTs);
}

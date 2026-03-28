import { subDays } from "date-fns";
import { prisma } from "@/lib/db/prisma";
import {
  buildRecommendation,
  classifyMarketRegime,
  computeCompetitionMetrics,
  computeWindowTrendSummary,
  estimateExpectedUtilization,
  estimateRoiContext,
} from "@/lib/metrics/intelligence";
import { estimatePowerAndCost } from "@/lib/scoring/hardwareDefaults";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ gpu: string }> },
) {
  const { gpu } = await params;
  const gpuName = decodeURIComponent(gpu);
  const url = new URL(request.url);
  const numGpusParam = url.searchParams.get("numGpus");
  const typeParam = url.searchParams.get("type");
  const numGpusFilter = numGpusParam ? Number(numGpusParam) : null;
  const offerTypeFilter = typeParam?.trim().toLowerCase() || null;

  const latestSnapshot = await prisma.marketSnapshot.findFirst({
    orderBy: { capturedAt: "desc" },
    select: { id: true, source: true, capturedAt: true },
  });

  if (!latestSnapshot) {
    return Response.json({
      gpuName,
      cohort: { numGpus: numGpusFilter, offerType: offerTypeFilter },
      points: [],
      trends: null,
      competition: null,
      regime: "balanced",
      recommendationLabel: "Wait",
      recommendationReasonPrimary: "No snapshots found.",
      recommendationReasonSecondary: "Run collection and recompute to generate segment metrics.",
      recommendationConfidenceNote: "No data",
      roi: null,
      latestHostMachineBreakdown: [],
    });
  }

  const since7d = subDays(latestSnapshot.capturedAt, 7);

  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: {
          source: string;
          gpuName: string;
          bucketStartUtc: { gte: Date };
          numGpus?: number;
          offerType?: string;
        };
        orderBy: { bucketStartUtc: "asc" };
        select: {
          id: true;
          source: true;
          gpuName: true;
          numGpus: true;
          offerType: true;
          bucketStartUtc: true;
          snapshotCount: true;
          totalOffers: true;
          rentableOffers: true;
          rentedOffers: true;
          minPrice: true;
          p10Price: true;
          medianPrice: true;
          p90Price: true;
          newOffers: true;
          disappearedOffers: true;
          newOfferRate: true;
          disappearedRate: true;
          cohortPressureScore: true;
          medianPriceChange: true;
          lowBandDisappearedRate: true;
          midBandDisappearedRate: true;
          highBandDisappearedRate: true;
        };
      }) => Promise<Array<{
        id: string;
        source: string;
        gpuName: string;
        numGpus: number;
        offerType: string;
        bucketStartUtc: Date;
        snapshotCount: number;
        totalOffers: number;
        rentableOffers: number;
        rentedOffers: number;
        minPrice: number | null;
        p10Price: number | null;
        medianPrice: number | null;
        p90Price: number | null;
        newOffers: number | null;
        disappearedOffers: number | null;
        newOfferRate: number | null;
        disappearedRate: number | null;
        cohortPressureScore: number | null;
        medianPriceChange: number | null;
        lowBandDisappearedRate: number | null;
        midBandDisappearedRate: number | null;
        highBandDisappearedRate: number | null;
      }>>;
    };
  }).gpuTrendAggregate;

  const points = trendClient
    ? await trendClient.findMany({
        where: {
          source: latestSnapshot.source,
          gpuName,
          ...(numGpusFilter == null ? {} : { numGpus: numGpusFilter }),
          ...(offerTypeFilter == null ? {} : { offerType: offerTypeFilter }),
          bucketStartUtc: { gte: since7d },
        },
        orderBy: { bucketStartUtc: "asc" },
        select: {
          id: true,
          source: true,
          gpuName: true,
          numGpus: true,
          offerType: true,
          bucketStartUtc: true,
          snapshotCount: true,
          totalOffers: true,
          rentableOffers: true,
          rentedOffers: true,
          minPrice: true,
          p10Price: true,
          medianPrice: true,
          p90Price: true,
          newOffers: true,
          disappearedOffers: true,
          newOfferRate: true,
          disappearedRate: true,
          cohortPressureScore: true,
          medianPriceChange: true,
          lowBandDisappearedRate: true,
          midBandDisappearedRate: true,
          highBandDisappearedRate: true,
        },
      })
    : [];

  const latestPoint = points.length > 0 ? points[points.length - 1] : null;
  const anchor = latestPoint?.bucketStartUtc ?? latestSnapshot.capturedAt;

  const trendLikePoints = points.map((point) => ({
    ...point,
  }));

  const trends = {
    window6h: computeWindowTrendSummary(trendLikePoints, anchor, "6h"),
    window24h: computeWindowTrendSummary(trendLikePoints, anchor, "24h"),
    window7d: computeWindowTrendSummary(trendLikePoints, anchor, "7d"),
  };

  const latestOffers = await prisma.offer.findMany({
    where: {
      snapshotId: latestSnapshot.id,
      gpuName,
      ...(numGpusFilter == null ? {} : { numGpus: numGpusFilter }),
      ...(offerTypeFilter == null ? {} : { offerType: offerTypeFilter }),
    },
    select: {
      hostId: true,
      machineId: true,
      numGpus: true,
      offerType: true,
      rentable: true,
      rented: true,
      pricePerHour: true,
      reliabilityScore: true,
    },
  });

  const competition = computeCompetitionMetrics(
    latestOffers.map((offer) => ({
      hostId: offer.hostId,
      machineId: offer.machineId,
      reliabilityScore: offer.reliabilityScore,
    })),
  );

  const hostMachineBreakdown = Array.from(
    latestOffers.reduce(
      (acc, offer) => {
        const key = `${offer.hostId ?? "unknown"}::${offer.machineId ?? "unknown"}`;
        const current = acc.get(key) ?? {
          hostId: offer.hostId,
          machineId: offer.machineId,
          totalOffers: 0,
          rentableOffers: 0,
          rentedOffers: 0,
          prices: [] as number[],
        };
        current.totalOffers += 1;
        current.rentableOffers += offer.rentable ? 1 : 0;
        current.rentedOffers += offer.rented ? 1 : 0;
        if (offer.pricePerHour != null) current.prices.push(offer.pricePerHour);
        acc.set(key, current);
        return acc;
      },
      new Map<
        string,
        {
          hostId: number | null;
          machineId: number | null;
          totalOffers: number;
          rentableOffers: number;
          rentedOffers: number;
          prices: number[];
        }
      >(),
    ).values(),
  )
    .map((row) => {
      const sorted = [...row.prices].sort((a, b) => a - b);
      const medianPrice = sorted.length === 0 ? null : sorted[Math.ceil(sorted.length * 0.5) - 1];
      return { ...row, medianPrice };
    })
    .sort((a, b) => b.totalOffers - a.totalOffers);

  const defaults = estimatePowerAndCost(gpuName);
  const listingPrice = latestPoint?.medianPrice ?? 0;
  const expectedUtilizationEstimate = estimateExpectedUtilization({
    disappearedRate: latestPoint?.disappearedRate ?? 0,
    netSupplyChange: (latestPoint?.newOffers ?? 0) - (latestPoint?.disappearedOffers ?? 0),
    visibleSupplyCount: latestPoint?.totalOffers ?? latestOffers.length,
    rentableShare:
      latestPoint == null
        ? latestOffers.length === 0
          ? 0
          : latestOffers.filter((offer) => offer.rentable).length / latestOffers.length
        : latestPoint.totalOffers === 0
          ? 0
          : latestPoint.rentableOffers / latestPoint.totalOffers,
    listingPricePerHour: listingPrice,
    medianPrice: latestPoint?.medianPrice ?? null,
    reliabilityScore: competition.avgReliabilityScore,
  });

  const roi = estimateRoiContext({
    expectedUtilizationEstimate,
    listingPricePerHour: listingPrice,
    hardwareCost: defaults.assumedHardwareCost,
    powerWatts: defaults.assumedPowerWatts,
  });

  const regime = classifyMarketRegime({
    disappearedRate: latestPoint?.disappearedRate ?? 0,
    newOfferRate: latestPoint?.newOfferRate ?? 0,
    netSupplyChange: (latestPoint?.newOffers ?? 0) - (latestPoint?.disappearedOffers ?? 0),
    medianPriceChange: latestPoint?.medianPriceChange ?? 0,
    rentableShareChange: 0,
    marketPressureScore: latestPoint?.cohortPressureScore ?? 0,
  });

  const recommendation = buildRecommendation({
    regime,
    lowBandDisappearedRate: latestPoint?.lowBandDisappearedRate ?? 0,
    midBandDisappearedRate: latestPoint?.midBandDisappearedRate ?? 0,
    highBandDisappearedRate: latestPoint?.highBandDisappearedRate ?? 0,
    topHostShare: competition.topHostShare,
    marketPressureScore: latestPoint?.cohortPressureScore ?? 0,
  });

  return Response.json({
    gpuName,
    cohort: {
      numGpus: numGpusFilter,
      offerType: offerTypeFilter,
    },
    points,
    trends,
    competition,
    regime,
    recommendationLabel: recommendation.recommendationLabel,
    recommendationReasonPrimary: recommendation.recommendationReasonPrimary,
    recommendationReasonSecondary: recommendation.recommendationReasonSecondary,
    recommendationConfidenceNote: recommendation.recommendationConfidenceNote,
    roi,
    latestHostMachineBreakdown: hostMachineBreakdown,
  });
}

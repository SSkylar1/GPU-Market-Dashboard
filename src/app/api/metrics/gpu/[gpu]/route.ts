import { subHours } from "date-fns";
import { prisma } from "@/lib/db/prisma";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ gpu: string }> },
) {
  const { gpu } = await params;
  const gpuName = decodeURIComponent(gpu);
  const since = subHours(new Date(), 24);

  const trendClient = (prisma as unknown as {
    gpuTrendAggregate?: {
      findMany: (args: {
        where: { gpuName: string; bucketStartUtc: { gte: Date } };
        orderBy: { bucketStartUtc: "asc" };
        select: {
          source: true;
          bucketStartUtc: true;
          snapshotCount: true;
          totalOffers: true;
          rentableOffers: true;
          rentedOffers: true;
          impliedUtilization: true;
          availabilityRatio: true;
          medianPrice: true;
          p90Price: true;
          minPrice: true;
        };
      }) => Promise<Array<{
        source: string;
        bucketStartUtc: Date;
        snapshotCount: number;
        totalOffers: number;
        rentableOffers: number;
        rentedOffers: number;
        impliedUtilization: number;
        availabilityRatio: number;
        medianPrice: number | null;
        p90Price: number | null;
        minPrice: number | null;
      }>>;
    };
  }).gpuTrendAggregate;

  const history = trendClient
    ? await trendClient.findMany({
        where: {
          gpuName,
          bucketStartUtc: {
            gte: since,
          },
        },
        orderBy: {
          bucketStartUtc: "asc",
        },
        select: {
          source: true,
          bucketStartUtc: true,
          snapshotCount: true,
          totalOffers: true,
          rentableOffers: true,
          rentedOffers: true,
          impliedUtilization: true,
          availabilityRatio: true,
          medianPrice: true,
          p90Price: true,
          minPrice: true,
        },
      })
    : [];

  const latestSnapshot = await prisma.marketSnapshot.findFirst({
    orderBy: { capturedAt: "desc" },
    select: { id: true },
  });
  const latestOffers = latestSnapshot
    ? await prisma.offer.findMany({
        where: {
          snapshotId: latestSnapshot.id,
          gpuName,
        },
        select: {
          hostId: true,
          machineId: true,
          rentable: true,
          rented: true,
          pricePerHour: true,
        },
      })
    : [];
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
        if (offer.pricePerHour != null) {
          current.prices.push(offer.pricePerHour);
        }
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
      const medianPrice =
        sorted.length === 0
          ? null
          : sorted[Math.ceil(sorted.length * 0.5) - 1];
      return {
        hostId: row.hostId,
        machineId: row.machineId,
        totalOffers: row.totalOffers,
        rentableOffers: row.rentableOffers,
        rentedOffers: row.rentedOffers,
        medianPrice,
      };
    })
    .sort((a, b) => b.totalOffers - a.totalOffers);

  return Response.json({
    gpuName,
    points: history.map((point) => ({
      ...point,
      availableShare: point.availabilityRatio,
      unavailableShare: point.impliedUtilization,
      leaseSignalShare: point.totalOffers === 0 ? 0 : point.rentedOffers / point.totalOffers,
      observedRentedShare: point.totalOffers === 0 ? 0 : point.rentedOffers / point.totalOffers,
    })),
    latestHostMachineBreakdown: hostMachineBreakdown,
  });
}

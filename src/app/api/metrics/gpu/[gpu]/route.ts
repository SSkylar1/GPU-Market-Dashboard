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

  return Response.json({
    gpuName,
    points: history,
  });
}

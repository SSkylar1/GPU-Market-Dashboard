import "dotenv/config";
import { PrismaClient } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

function minutesAgo(value: Date | null): number | null {
  if (!value) return null;
  return Math.max(0, Math.floor((Date.now() - value.getTime()) / 60000));
}

async function main() {
  const [latestSnapshot, latestBucket] = await Promise.all([
    prisma.marketSnapshot.findFirst({ orderBy: { capturedAt: "desc" } }),
    prisma.gpuTrendAggregate.findFirst({ orderBy: { bucketStartUtc: "desc" } }),
  ]);

  console.log("Pipeline status");
  console.log(`- latest snapshot id: ${latestSnapshot?.id ?? "none"}`);
  console.log(`- latest snapshot at (UTC): ${latestSnapshot?.capturedAt.toISOString() ?? "none"}`);
  console.log(
    `- snapshot lag minutes: ${
      latestSnapshot ? String(minutesAgo(latestSnapshot.capturedAt)) : "n/a"
    }`,
  );
  console.log(`- latest aggregate bucket (UTC): ${latestBucket?.bucketStartUtc.toISOString() ?? "none"}`);
  console.log(
    `- aggregate lag minutes: ${
      latestBucket ? String(minutesAgo(latestBucket.bucketStartUtc)) : "n/a"
    }`,
  );
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

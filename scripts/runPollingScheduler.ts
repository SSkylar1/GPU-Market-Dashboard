import "dotenv/config";
import { spawnSync } from "node:child_process";
import { PrismaClient } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import {
  getPollingWindowForTier,
  loadPollingConfig,
  resolvePollingTier,
  type PollingTier,
} from "../src/lib/ingestion/polling";

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

function runCommand(command: string, args: string[]): void {
  const result = spawnSync(command, args, { stdio: "inherit", env: process.env });
  if (result.status !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(" ")}`);
  }
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const config = loadPollingConfig();
  const latestBySource = await prisma.marketSnapshot.groupBy({
    by: ["source"],
    _max: { capturedAt: true },
  });

  const latestCohorts = await prisma.gpuTrendAggregate.findMany({
    where: { bucketStartUtc: { gte: new Date(Date.now() - 6 * 3600 * 1000) } },
    select: { source: true, gpuName: true, numGpus: true, offerType: true, bucketStartUtc: true },
    orderBy: { bucketStartUtc: "desc" },
    take: 1200,
  });

  const cohortsBySource = new Map<string, Array<{ gpuName: string | null }>>();
  for (const row of latestCohorts) {
    const current = cohortsBySource.get(row.source) ?? [];
    current.push({ gpuName: row.gpuName });
    cohortsBySource.set(row.source, current);
  }

  let shouldRunPipeline = false;
  for (const sourceRow of latestBySource) {
    const source = sourceRow.source;
    const lastCapturedAt = sourceRow._max.capturedAt;
    if (!lastCapturedAt) continue;

    const cohorts = cohortsBySource.get(source) ?? [];
    let selectedTier: PollingTier = resolvePollingTier(source, null, config);
    for (const cohort of cohorts) {
      const tier = resolvePollingTier(source, cohort.gpuName, config);
      if (tier === "high-priority") {
        selectedTier = tier;
        break;
      }
      if (tier === "general" && selectedTier === "long-tail") {
        selectedTier = tier;
      }
    }

    const window = getPollingWindowForTier(source, selectedTier, config);
    const lagMinutes = Math.max(0, (Date.now() - lastCapturedAt.getTime()) / 60000);
    const due = lagMinutes >= window.targetMinutes;
    console.log(
      `[scheduler] source=${source} tier=${selectedTier} lag=${lagMinutes.toFixed(1)}m target=${window.targetMinutes}m due=${due}`,
    );
    if (due) shouldRunPipeline = true;
  }

  if (!shouldRunPipeline) {
    console.log("[scheduler] no source due for polling.");
    return;
  }

  if (dryRun) {
    console.log("[scheduler] dry-run: skipping collect/recompute.");
    return;
  }

  runCommand("npm", ["run", "collect"]);
  runCommand("npm", ["run", "recompute"]);
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

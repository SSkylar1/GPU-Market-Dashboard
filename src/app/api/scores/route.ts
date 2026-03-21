import { z } from "zod";
import { calculateScenarioScore } from "@/lib/scoring/score";

const scoreInputSchema = z.object({
  demandScore: z.number().min(0).max(100),
  priceStrengthScore: z.number().min(0).max(100),
  competitionScore: z.number().min(0).max(100),
  efficiencyScore: z.number().min(0).max(100),
});

export async function POST(request: Request) {
  const payload = await request.json();
  const parsed = scoreInputSchema.safeParse(payload);

  if (!parsed.success) {
    return Response.json(
      { error: "Invalid scoring payload", issues: parsed.error.issues },
      { status: 400 },
    );
  }

  return Response.json(calculateScenarioScore(parsed.data));
}

import { getRecommendedPriceBands } from "@/lib/scoring/pricing";

export default function PricingPage() {
  const bands = getRecommendedPriceBands([0.62, 0.71, 0.69, 0.74, 0.81, 0.66]);

  return (
    <main className="mx-auto w-full max-w-3xl p-6 md:p-10">
      <h1 className="mb-4 text-2xl font-semibold">Pricing Recommendations</h1>
      <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm">
        <p>Aggressive: ${bands.aggressive.toFixed(3)}/hr</p>
        <p>Target: ${bands.target.toFixed(3)}/hr</p>
        <p>Premium: ${bands.premium.toFixed(3)}/hr</p>
      </div>
    </main>
  );
}

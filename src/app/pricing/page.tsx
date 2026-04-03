import { getRecommendedPriceBands } from "@/lib/scoring/pricing";
import Link from "next/link";

export default function PricingPage() {
  const bands = getRecommendedPriceBands([0.62, 0.71, 0.69, 0.74, 0.81, 0.66]);

  return (
    <main className="mx-auto w-full max-w-3xl p-6 md:p-10">
      <header className="mb-4 flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold text-zinc-100">Pricing Recommendations</h1>
        <nav className="flex gap-3 text-sm">
          <Link className="text-blue-400 underline" href="/market">
            Market
          </Link>
          <Link className="text-blue-400 underline" href="/scoring">
            Scoring
          </Link>
          <Link className="text-blue-400 underline" href="/ops">
            Ops
          </Link>
        </nav>
      </header>
      <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-900">
        <p>Aggressive: ${bands.aggressive.toFixed(3)}/hr</p>
        <p>Target: ${bands.target.toFixed(3)}/hr</p>
        <p>Premium: ${bands.premium.toFixed(3)}/hr</p>
      </div>
    </main>
  );
}

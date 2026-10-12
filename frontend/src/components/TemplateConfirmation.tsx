import type { ComponentType } from "react";
import { ArrowRight, BarChart3, BriefcaseBusiness, CheckCircle2, Globe, HeartPulse, MessageSquareQuote, ShoppingBag, Sparkles, TrendingUp } from "lucide-react";
import type { BusinessContext, DetectionCandidate, TemplateOption } from "../types";
import { cn } from "../lib/cn";

type TemplateConfirmationProps = {
  candidates: DetectionCandidate[];
  detectedTemplate: BusinessContext | null;
  templateOptions: TemplateOption[];
  selectedKind: string;
  onSelectedKindChange: (nextKind: string) => void;
  onApplyOverride: () => void;
  onContinue: () => void;
  isApplying: boolean;
};

const TEMPLATE_ICONS: Record<string, ComponentType<{ className?: string }>> = {
  financial_timeseries: TrendingUp,
  ecommerce_orders: ShoppingBag,
  healthcare_medical: HeartPulse,
  marketing_campaign: Sparkles,
  hr_workforce: BriefcaseBusiness,
  survey_sentiment: MessageSquareQuote,
  web_app_analytics: Globe,
  generic: BarChart3,
};

function nearMissText(candidate: DetectionCandidate): string {
  if (candidate.missing.length === 0) {
    return "The columns are here, but too few rows have usable values.";
  }
  const shown = candidate.missing.slice(0, 5).join(", ").replace(/_/g, " ");
  const more = candidate.missing.length > 5 ? `, and ${candidate.missing.length - 5} more` : "";
  return `Not matched. Its closest accepted shape is missing: ${shown}${more}.`;
}

export function TemplateConfirmation({
  candidates,
  detectedTemplate,
  templateOptions,
  selectedKind,
  onSelectedKindChange,
  onApplyOverride,
  onContinue,
  isApplying,
}: TemplateConfirmationProps) {
  const matchLabel = !detectedTemplate
    ? "No specialized match"
    : detectedTemplate.match
      ? `Matched ${detectedTemplate.match.found} of ${detectedTemplate.match.expected} expected columns`
      : "Template matched";
  const candidatesByKind = new Map(candidates.map((candidate) => [candidate.kind, candidate]));

  return (
    <div className="mx-auto flex min-h-screen w-full max-w-4xl items-center px-6 py-16" data-testid="template-screen">
      <div className="panel w-full p-8 md:p-10">
        <div className="mb-8 flex flex-wrap items-center gap-3">
          <span className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            {matchLabel}
          </span>
        </div>

        <h2 className="editorial-heading text-3xl font-bold">Confirm the business template</h2>
        <p className="mt-3 max-w-2xl text-base leading-7 text-stone-600" data-testid="template-explanation">
          {detectedTemplate
            ? "Loom matched the dataset to a specialized template. Keep it if it looks right, or override it before the insight review begins."
            : "No specialized template matched this dataset, so Loom will build a general dashboard from its columns. If you know one of the templates fits, pick it and apply the override."}
        </p>

        <div className="mt-8 grid gap-4">
          {templateOptions.map((option) => {
            const Icon = TEMPLATE_ICONS[option.kind] ?? BarChart3;
            const selected = option.kind === selectedKind;
            const candidate = candidatesByKind.get(option.kind);

            return (
              <button
                className={cn(
                  "flex w-full items-start gap-4 rounded-2xl border p-5 text-left transition",
                  selected
                    ? "border-[#c2410c] bg-[#fff7ed]/80 shadow-sm"
                    : "border-stone-200 bg-white/90 hover:border-stone-300 hover:bg-stone-50/90",
                  !option.implemented && "opacity-60"
                )}
                data-testid={`template-option-${option.kind}`}
                disabled={!option.implemented}
                key={option.kind}
                onClick={() => onSelectedKindChange(option.kind)}
                type="button"
              >
                <div
                  className={cn(
                    "rounded-xl p-3",
                    selected ? "bg-[#292524] text-[#fafaf9]" : "bg-stone-100 text-stone-600"
                  )}
                >
                  <Icon className="h-5 w-5" />
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between gap-3">
                    <h3 className="font-display text-2xl font-bold text-stone-900">{option.label}</h3>
                    {selected ? <CheckCircle2 className="h-5 w-5 text-[#c2410c]" /> : null}
                  </div>
                  <p className="mt-2 text-sm leading-6 text-stone-500">{option.description}</p>
                  {candidate && option.kind !== detectedTemplate?.kind ? (
                    <p className="mt-2 text-xs leading-5 text-stone-400" data-testid={`template-near-miss-${option.kind}`}>
                      {nearMissText(candidate)}
                    </p>
                  ) : null}
                </div>
              </button>
            );
          })}
        </div>

        <div className="mt-8 flex flex-col gap-3 sm:flex-row">
          <button className="secondary-button" data-testid="apply-template-override-button" disabled={isApplying} onClick={onApplyOverride}>
            {isApplying ? "Applying..." : "Apply override"}
          </button>
          <button className="primary-button" data-testid="continue-to-review-button" onClick={onContinue}>
            Continue to insight review
            <ArrowRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

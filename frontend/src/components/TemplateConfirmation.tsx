import type { ComponentType } from "react";
import { ArrowRight, BarChart3, CheckCircle2, ShoppingBag, TrendingUp } from "lucide-react";
import type { BusinessContext, TemplateOption } from "../types";
import { cn } from "../lib/cn";

type TemplateConfirmationProps = {
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
  generic: BarChart3,
};

export function TemplateConfirmation({
  detectedTemplate,
  templateOptions,
  selectedKind,
  onSelectedKindChange,
  onApplyOverride,
  onContinue,
  isApplying,
}: TemplateConfirmationProps) {
  const confidence = detectedTemplate ? Math.round(detectedTemplate.confidence * 100) : 0;

  return (
    <div className="mx-auto flex min-h-screen w-full max-w-4xl items-center px-6 py-16">
      <div className="panel w-full p-8 md:p-10">
        <div className="mb-8 flex flex-wrap items-center gap-3">
          <span className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            {detectedTemplate ? `${confidence}% detection confidence` : "Template review"}
          </span>
        </div>

        <h2 className="editorial-heading text-3xl font-bold">Confirm the business template</h2>
        <p className="mt-3 max-w-2xl text-base leading-7 text-stone-600">
          Loom matched the dataset to a specialized template. Keep it if it looks right, or override it before the
          insight review begins.
        </p>

        <div className="mt-8 grid gap-4">
          {templateOptions.map((option) => {
            const Icon = TEMPLATE_ICONS[option.kind] ?? BarChart3;
            const selected = option.kind === selectedKind;

            return (
              <button
                className={cn(
                  "flex w-full items-start gap-4 rounded-2xl border p-5 text-left transition",
                  selected
                    ? "border-[#c2410c] bg-[#fff7ed]/80 shadow-sm"
                    : "border-stone-200 bg-white/90 hover:border-stone-300 hover:bg-stone-50/90",
                  !option.implemented && "opacity-60"
                )}
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
                </div>
              </button>
            );
          })}
        </div>

        <div className="mt-8 flex flex-col gap-3 sm:flex-row">
          <button className="secondary-button" disabled={isApplying} onClick={onApplyOverride}>
            {isApplying ? "Applying..." : "Apply override"}
          </button>
          <button className="primary-button" onClick={onContinue}>
            Continue to insight review
            <ArrowRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

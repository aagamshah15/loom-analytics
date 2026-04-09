import { ArrowLeft, ArrowRight, CheckCircle2, House, Info, Sparkles, Wand2, XCircle } from "lucide-react";
import { RunSummary } from "./RunSummary";
import type { AnalyzeReport, InsightCandidate } from "../types";
import { cn } from "../lib/cn";

type InsightReviewProps = {
  report: AnalyzeReport;
  inputName: string;
  insights: InsightCandidate[];
  approvals: Record<string, boolean>;
  onToggleInsight: (insightId: string, nextValue: boolean) => void;
  userPrompt: string;
  onPromptChange: (nextValue: string) => void;
  onApplyPrompt: () => void;
  onApproveAll: () => void;
  onRejectAll: () => void;
  focusTags: string[];
  onBackToTemplate: () => void;
  onBackToLanding: () => void;
  onBuildDashboard: () => void;
  isApplyingPrompt: boolean;
  isBuilding: boolean;
  approvedCount: number;
};

export function InsightReview({
  report,
  inputName,
  insights,
  approvals,
  onToggleInsight,
  userPrompt,
  onPromptChange,
  onApplyPrompt,
  onApproveAll,
  onRejectAll,
  focusTags,
  onBackToTemplate,
  onBackToLanding,
  onBuildDashboard,
  isApplyingPrompt,
  isBuilding,
  approvedCount,
}: InsightReviewProps) {
  return (
    <div className="min-h-screen" data-testid="review-screen">
      <div className="mx-auto w-full max-w-7xl px-6 py-10">
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <button className="ghost-button" data-testid="back-to-template-button" onClick={onBackToTemplate}>
            <ArrowLeft className="h-4 w-4" />
            Back to template
          </button>
          <button className="ghost-button" data-testid="start-over-review-button" onClick={onBackToLanding}>
            <House className="h-4 w-4" />
            Start over
          </button>
        </div>

        <div className="mb-8">
          <RunSummary inputName={inputName} report={report} />
        </div>

        <div className="mb-8 flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="section-heading">Step 2 · Insight Review</p>
            <h2 className="editorial-heading mt-3 text-4xl font-bold">Curate the findings worth turning into a dashboard</h2>
            <p className="mt-4 max-w-3xl text-base leading-7 text-stone-600">
              Approve or reject individual insights, steer the analysis with a prompt, and build the dashboard only when
              the story looks right.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <button className="secondary-button" data-testid="approve-all-button" onClick={onApproveAll}>
              <CheckCircle2 className="h-4 w-4" />
              Approve all
            </button>
            <button className="secondary-button" data-testid="reject-all-button" onClick={onRejectAll}>
              <XCircle className="h-4 w-4" />
              Reject all
            </button>
            <button
              className="primary-button"
              data-testid="build-dashboard-button"
              disabled={approvedCount === 0 || isBuilding}
              onClick={onBuildDashboard}
            >
              {isBuilding ? "Building..." : "Build dashboard"}
              <ArrowRight className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="mb-8 grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
          <div className="panel p-6">
            <div className="mb-4 flex items-center gap-2">
              <div className="rounded-xl bg-[#fff7ed] p-2 text-[#c2410c]">
                <Wand2 className="h-4 w-4" />
              </div>
              <div>
                <p className="text-sm font-semibold text-stone-900">Steer the next analysis pass</p>
                <p className="text-sm text-stone-500">Use plain English to shift emphasis before you build.</p>
              </div>
            </div>
            <textarea
              className="loom-textarea"
              data-testid="review-prompt-input"
              onChange={(event) => onPromptChange(event.target.value)}
              placeholder="Example: focus on volatility, make this executive-friendly, or emphasize discount inefficiency."
              value={userPrompt}
            />
            <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex flex-wrap gap-2">
                {focusTags.map((tag) => (
                  <span className="loom-chip" key={tag}>
                    {tag}
                  </span>
                ))}
              </div>
              <button className="secondary-button" data-testid="apply-instructions-button" disabled={isApplyingPrompt} onClick={onApplyPrompt}>
                <Sparkles className="h-4 w-4" />
                {isApplyingPrompt ? "Refreshing..." : "Apply instructions"}
              </button>
            </div>
          </div>

          <div className="panel p-6">
            <p className="section-heading">Approval status</p>
            <div className="mt-4 grid gap-4 sm:grid-cols-2">
              <StatusCard label="Approved" tone="positive" value={approvedCount} />
              <StatusCard label="Rejected" tone="default" value={insights.length - approvedCount} />
            </div>
            <div className="mt-4 rounded-2xl border border-stone-200 bg-[#f5f5f4]/85 px-4 py-4 text-sm leading-6 text-stone-600">
              The builder uses only the approved insight set as its blueprint. Applying new instructions refreshes the
              suggested approvals.
            </div>
          </div>
        </div>

        <div className="grid gap-5 lg:grid-cols-2">
          {insights.map((insight) => {
            const approved = approvals[insight.id];
            return (
              <article
                className={cn(
                  "panel p-6 transition",
                  approved ? "border-[#fdba74] shadow-sm" : "border-stone-200 opacity-75"
                )}
                data-testid={`insight-card-${insight.id}`}
                key={insight.id}
              >
                <div className="mb-5 flex items-start justify-between gap-4">
                  <div className="flex items-center gap-2">
                    <span className="rounded-full border border-stone-200 bg-stone-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-stone-600">
                      {insight.category}
                    </span>
                    <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold uppercase tracking-wide text-stone-500">
                      {insight.severity}
                    </span>
                  </div>
                  <button
                    className={cn(
                      "inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold transition",
                      approved ? "bg-[#292524] text-[#fafaf9]" : "border border-stone-200 bg-white text-stone-700"
                    )}
                    data-testid={`toggle-insight-${insight.id}`}
                    onClick={() => onToggleInsight(insight.id, !approved)}
                  >
                    {approved ? <CheckCircle2 className="h-4 w-4" /> : <Info className="h-4 w-4" />}
                    {approved ? "Approved" : "Rejected"}
                  </button>
                </div>

                <h3 className="font-display text-2xl font-bold tracking-tight text-stone-900">{insight.title}</h3>
                <p className="mt-3 text-sm font-medium leading-6 text-stone-700">{insight.summary}</p>
                <p className="mt-3 text-sm leading-6 text-stone-500">{insight.detail}</p>

                <div className="mt-5 rounded-2xl border border-stone-200 bg-[#f5f5f4]/85 px-4 py-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{insight.metric_label}</p>
                  <p className="mt-2 text-2xl font-bold tracking-tight text-stone-900">{insight.metric_value}</p>
                  <p className="mt-1 text-sm text-stone-500">{insight.metric_sub}</p>
                </div>
              </article>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function StatusCard({ label, value, tone }: { label: string; value: number; tone: "positive" | "default" }) {
  return (
    <div className="rounded-2xl border border-stone-200 bg-white px-4 py-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{label}</p>
      <p className={cn("mt-2 text-2xl font-bold", tone === "positive" ? "text-emerald-600" : "text-stone-900")}>
        {value}
      </p>
    </div>
  );
}

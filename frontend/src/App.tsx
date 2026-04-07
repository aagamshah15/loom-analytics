import { useEffect, useMemo, useState } from "react";
import { ArrowLeft, Download, Eye } from "lucide-react";
import { analyzeCsv, buildDashboard, regenerateReview } from "./api/client";
import { DashboardBuilder } from "./components/DashboardBuilder";
import { DashboardCanvas } from "./components/dashboard/DashboardCanvas";
import { InsightReview } from "./components/InsightReview";
import { LoadingOverlay } from "./components/LoadingOverlay";
import { TemplateConfirmation } from "./components/TemplateConfirmation";
import { UploadHero } from "./components/UploadHero";
import { downloadHtmlFile, rebuildDashboardBlueprint } from "./lib/dashboard";
import type { AnalyzeResponse, DashboardBlueprint, DashboardResponse } from "./types";

type ScreenStep = "landing" | "loading" | "template" | "review" | "builder" | "preview";

const LOADING_STAGES = [
  "Untangling raw data...",
  "Sorting structure and quality...",
  "Cleaning inconsistent values...",
  "Matching the best template...",
  "Weaving the first insight set...",
];

function App() {
  const [step, setStep] = useState<ScreenStep>("landing");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [analysis, setAnalysis] = useState<AnalyzeResponse | null>(null);
  const [selectedTemplateKind, setSelectedTemplateKind] = useState("generic");
  const [approvals, setApprovals] = useState<Record<string, boolean>>({});
  const [userPrompt, setUserPrompt] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loadingProgress, setLoadingProgress] = useState(0);
  const [loadingStageIndex, setLoadingStageIndex] = useState(0);
  const [isApplyingTemplate, setIsApplyingTemplate] = useState(false);
  const [isBuildingDashboard, setIsBuildingDashboard] = useState(false);
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [dashboardTitle, setDashboardTitle] = useState("");
  const [dashboardSubtitle, setDashboardSubtitle] = useState("");
  const [sectionOrder, setSectionOrder] = useState<string[]>([]);
  const [metricCount, setMetricCount] = useState(4);
  const [showNotes, setShowNotes] = useState(true);
  const [isSyncingPreview, setIsSyncingPreview] = useState(false);

  const businessContext = analysis?.business_context ?? null;
  const review = analysis?.review ?? { insights: [], focus_tags: [] };
  const templateOptions = analysis?.template_options ?? [];

  const approvedInsightIds = useMemo(
    () => review.insights.filter((insight) => approvals[insight.id]).map((insight) => insight.id),
    [approvals, review.insights]
  );

  const maxMetricCount = Math.max(2, Math.min(6, Math.max(approvedInsightIds.length, 2)));

  useEffect(() => {
    if (step !== "loading") {
      return;
    }

    setLoadingProgress(12);
    setLoadingStageIndex(0);

    const progressTimer = window.setInterval(() => {
      setLoadingProgress((current) => {
        if (current >= 92) {
          return current;
        }
        return Math.min(current + 6, 92);
      });
    }, 220);

    const stageTimer = window.setInterval(() => {
      setLoadingStageIndex((current) => Math.min(current + 1, LOADING_STAGES.length - 1));
    }, 850);

    return () => {
      window.clearInterval(progressTimer);
      window.clearInterval(stageTimer);
    };
  }, [step]);

  const draftBlueprint = useMemo<DashboardBlueprint | null>(() => {
    if (!dashboard?.blueprint) {
      return null;
    }

    return rebuildDashboardBlueprint(dashboard.blueprint, {
      title: dashboardTitle,
      subtitle: dashboardSubtitle,
      sectionOrder,
      metricCount: Math.min(metricCount, maxMetricCount),
      showNotes,
    });
  }, [dashboard, dashboardTitle, dashboardSubtitle, metricCount, sectionOrder, showNotes, maxMetricCount]);

  async function runAnalyze(templateOverride?: string, nextStep: ScreenStep = "template") {
    if (!selectedFile) {
      return;
    }

    try {
      setError(null);
      setDashboard(null);
      setStep("loading");
      const response = await analyzeCsv(selectedFile, templateOverride);
      setLoadingProgress(100);
      setAnalysis(response);

      const kind = response.business_context?.kind ?? response.detected_template?.kind ?? "generic";
      setSelectedTemplateKind(kind);
      setApprovals(
        Object.fromEntries(response.review.insights.map((insight) => [insight.id, Boolean(insight.recommended)]))
      );
      setUserPrompt("");
      setDashboardTitle(defaultDashboardTitle(kind));
      setDashboardSubtitle(
        response.business_context
          ? `Approved insight blueprint for ${response.business_context.display_name}`
          : "Approved insight blueprint"
      );
      setSectionOrder(defaultSectionsForReview(response.review.insights));
      setMetricCount(4);
      setShowNotes(true);

      window.setTimeout(() => {
        setStep(nextStep);
      }, 250);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Something went wrong during analysis.");
      setStep("landing");
    }
  }

  async function handleConfirmTemplate() {
    if (!analysis) {
      return;
    }

    const currentKind = analysis.business_context?.kind ?? analysis.detected_template?.kind ?? "generic";
    if (selectedTemplateKind !== currentKind) {
      setIsApplyingTemplate(true);
      await runAnalyze(selectedTemplateKind, "review");
      setIsApplyingTemplate(false);
      return;
    }
    setStep("review");
  }

  async function handleApplyPrompt() {
    if (!businessContext) {
      return;
    }

    try {
      setError(null);
      const refreshedReview = await regenerateReview(businessContext.kind, businessContext.analysis, userPrompt);
      setAnalysis((current) => (current ? { ...current, review: refreshedReview } : current));
      setApprovals((current) => {
        const nextApprovals: Record<string, boolean> = {};
        for (const insight of refreshedReview.insights) {
          nextApprovals[insight.id] = current[insight.id] ?? Boolean(insight.recommended);
        }
        return nextApprovals;
      });
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Unable to refresh insight review.");
    }
  }

  async function handleBuildDashboard(nextStep: ScreenStep = "builder") {
    if (!businessContext || approvedInsightIds.length === 0) {
      return;
    }

    try {
      setError(null);
      if (nextStep === "preview") {
        setIsSyncingPreview(true);
      } else {
        setIsBuildingDashboard(true);
      }

      const response = await buildDashboard({
        kind: businessContext.kind,
        analysis: businessContext.analysis,
        approvedInsightIds,
        userPrompt,
        settings: {
          title: dashboardTitle,
          subtitle: dashboardSubtitle,
          includedSections: sectionOrder,
          metricCount: Math.min(metricCount, maxMetricCount),
          showNotes,
        },
      });

      setDashboard(response);
      setDashboardTitle(response.blueprint.title);
      setDashboardSubtitle(response.blueprint.subtitle);
      setSectionOrder(response.blueprint.layout_sections.map((section) => section.id));
      setMetricCount(Math.min(metricCount, maxMetricCount));
      setShowNotes(response.blueprint.show_notes);
      setStep(nextStep);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Unable to build the dashboard.");
    } finally {
      setIsBuildingDashboard(false);
      setIsSyncingPreview(false);
    }
  }

  return (
    <div className="min-h-screen">
      {error ? (
        <div className="fixed left-1/2 top-4 z-30 -translate-x-1/2 rounded-full border border-rose-200 bg-rose-50 px-5 py-3 text-sm text-rose-700 shadow-sm">
          {error}
        </div>
      ) : null}

      {step === "landing" ? (
        <UploadHero disabled={!selectedFile} file={selectedFile} onAnalyze={() => runAnalyze()} onFileSelected={setSelectedFile} />
      ) : null}

      {step === "loading" ? <LoadingOverlay progress={loadingProgress} stage={LOADING_STAGES[loadingStageIndex]} /> : null}

      {step === "template" && analysis ? (
        <TemplateConfirmation
          detectedTemplate={analysis.detected_template}
          isApplying={isApplyingTemplate}
          onApplyOverride={handleConfirmTemplate}
          onContinue={handleConfirmTemplate}
          onSelectedKindChange={setSelectedTemplateKind}
          selectedKind={selectedTemplateKind}
          templateOptions={templateOptions}
        />
      ) : null}

      {step === "review" && analysis ? (
        businessContext ? (
          <InsightReview
            approvals={approvals}
            approvedCount={approvedInsightIds.length}
            focusTags={review.focus_tags}
            inputName={selectedFile?.name ?? "CSV file"}
            insights={review.insights}
            isBuilding={isBuildingDashboard}
            onApplyPrompt={handleApplyPrompt}
            onApproveAll={() =>
              setApprovals(Object.fromEntries(review.insights.map((insight) => [insight.id, true])))
            }
            onBuildDashboard={() => handleBuildDashboard("builder")}
            onPromptChange={setUserPrompt}
            onRejectAll={() =>
              setApprovals(Object.fromEntries(review.insights.map((insight) => [insight.id, false])))
            }
            onToggleInsight={(insightId, nextValue) =>
              setApprovals((current) => ({
                ...current,
                [insightId]: nextValue,
              }))
            }
            report={analysis.report}
            userPrompt={userPrompt}
          />
        ) : (
          <UnsupportedTemplateState onRestart={() => setStep("landing")} />
        )
      ) : null}

      {step === "builder" && draftBlueprint ? (
        <DashboardBuilder
          availableSections={sectionLabelMapForKind(draftBlueprint.kind, templateOptions)}
          blueprint={draftBlueprint}
          isSyncingPreview={isSyncingPreview}
          maxMetricCount={maxMetricCount}
          metricCount={metricCount}
          onBackToReview={() => setStep("review")}
          onMetricCountChange={setMetricCount}
          onOpenPreview={() => handleBuildDashboard("preview")}
          onSectionOrderChange={setSectionOrder}
          onShowNotesChange={setShowNotes}
          onSubtitleChange={setDashboardSubtitle}
          onTitleChange={setDashboardTitle}
          sectionOrder={sectionOrder}
          showNotes={showNotes}
          subtitle={dashboardSubtitle}
          title={dashboardTitle}
        />
      ) : null}

      {step === "preview" && draftBlueprint && dashboard ? (
        <div className="min-h-screen">
          <header className="sticky top-0 z-20 border-b border-stone-200 bg-[#fafaf9]/90 backdrop-blur">
            <div className="mx-auto flex w-full max-w-[1440px] items-center justify-between px-6 py-4">
              <div className="flex items-center gap-3">
                <button className="ghost-button" onClick={() => setStep("builder")}>
                  <ArrowLeft className="h-4 w-4" />
                  Builder
                </button>
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">Preview</p>
                  <h2 className="font-display text-2xl font-bold text-stone-900">{dashboardTitle}</h2>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <button className="secondary-button" onClick={() => setStep("builder")}>
                  <Eye className="h-4 w-4" />
                  Back to builder
                </button>
                <button className="primary-button" onClick={() => downloadHtmlFile(dashboard.download_name, dashboard.html)}>
                  <Download className="h-4 w-4" />
                  Download static HTML
                </button>
              </div>
            </div>
          </header>

          <DashboardCanvas blueprint={draftBlueprint} mode="preview" />
        </div>
      ) : null}
    </div>
  );
}

function UnsupportedTemplateState({ onRestart }: { onRestart: () => void }) {
  return (
    <div className="mx-auto flex min-h-screen w-full max-w-3xl items-center px-6">
      <div className="panel w-full p-8 text-center">
        <p className="section-heading">Template coming soon</p>
        <h2 className="editorial-heading mt-4 text-3xl font-bold">No specialized template is ready for this dataset yet</h2>
        <p className="mt-4 text-base leading-7 text-stone-600">
          The new product UI is in place, but this release only supports financial time-series and e-commerce order
          datasets with full native dashboard rendering.
        </p>
        <button className="primary-button mt-8" onClick={onRestart}>
          Start another upload
        </button>
      </div>
    </div>
  );
}

function defaultDashboardTitle(kind: string): string {
  if (kind === "financial_timeseries") {
    return "Hidden Market Structure";
  }
  if (kind === "ecommerce_orders") {
    return "E-commerce Hidden Insights";
  }
  return "Business Dashboard";
}

function defaultSectionsForReview(insights: AnalyzeResponse["review"]["insights"]): string[] {
  const sections = insights.map((insight) => insight.section);
  return Array.from(new Set(["overview", ...sections]));
}

function sectionLabelMapForKind(kind: string, options: AnalyzeResponse["template_options"]): Record<string, string> {
  if (kind === "financial_timeseries") {
    return {
      overview: "Narrative headline cards",
      seasonality: "Weekday and month patterns",
      volatility: "Volatility regime",
      gaps: "Overnight gap behavior",
      volume: "Extreme volume",
      data_notes: "Approved insight notes",
    };
  }

  if (kind === "ecommerce_orders") {
    return {
      overview: "Narrative KPI cards",
      revenue: "Revenue patterns",
      returns: "Return behavior",
      channels: "Channel and device quality",
      discounts: "Discount efficiency",
      notes: "Approved insight notes",
    };
  }

  return Object.fromEntries(options.filter((option) => option.implemented).map((option) => [option.kind, option.label]));
}

export default App;

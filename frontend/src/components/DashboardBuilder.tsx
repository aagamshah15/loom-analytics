import { ArrowDown, ArrowLeft, ArrowUp, Eye, House, LayoutDashboard, ToggleLeft, ToggleRight } from "lucide-react";
import type { DashboardBlueprint } from "../types";
import { cn } from "../lib/cn";
import { DashboardCanvas } from "./dashboard/DashboardCanvas";

type DashboardBuilderProps = {
  blueprint: DashboardBlueprint;
  title: string;
  subtitle: string;
  sectionOrder: string[];
  availableSections: Record<string, string>;
  metricCount: number;
  showNotes: boolean;
  onTitleChange: (value: string) => void;
  onSubtitleChange: (value: string) => void;
  onSectionOrderChange: (nextOrder: string[]) => void;
  onMetricCountChange: (value: number) => void;
  onShowNotesChange: (value: boolean) => void;
  onBackToLanding: () => void;
  onBackToReview: () => void;
  onOpenPreview: () => void;
  isSyncingPreview: boolean;
  maxMetricCount: number;
};

export function DashboardBuilder({
  blueprint,
  title,
  subtitle,
  sectionOrder,
  availableSections,
  metricCount,
  showNotes,
  onTitleChange,
  onSubtitleChange,
  onSectionOrderChange,
  onMetricCountChange,
  onShowNotesChange,
  onBackToLanding,
  onBackToReview,
  onOpenPreview,
  isSyncingPreview,
  maxMetricCount,
}: DashboardBuilderProps) {
  const orderedSectionIds = sectionOrder.filter((sectionId) => sectionId in availableSections);
  const inactiveSectionIds = Object.keys(availableSections).filter((sectionId) => !sectionOrder.includes(sectionId));

  function moveSection(sectionId: string, direction: "up" | "down") {
    const index = sectionOrder.indexOf(sectionId);
    if (index === -1) {
      return;
    }

    const nextIndex = direction === "up" ? index - 1 : index + 1;
    if (nextIndex < 0 || nextIndex >= sectionOrder.length) {
      return;
    }

    const nextOrder = [...sectionOrder];
    const [item] = nextOrder.splice(index, 1);
    nextOrder.splice(nextIndex, 0, item);
    onSectionOrderChange(nextOrder);
  }

  return (
    <div className="min-h-screen" data-testid="builder-screen">
      <header className="sticky top-0 z-20 border-b border-stone-200 bg-[#fafaf9]/90 backdrop-blur">
        <div className="mx-auto flex w-full max-w-[1440px] items-center justify-between px-6 py-4">
          <div className="flex items-center gap-4">
            <button className="ghost-button" data-testid="back-to-review-button" onClick={onBackToReview}>
              <ArrowLeft className="h-4 w-4" />
              Review
            </button>
            <div className="rounded-xl bg-[#292524] p-2 text-[#fafaf9]">
              <LayoutDashboard className="h-5 w-5" />
            </div>
            <div>
              <h1 className="font-display text-2xl font-bold tracking-tight text-stone-900">{title}</h1>
              <p className="text-sm text-stone-500">Draft builder · session only</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button className="secondary-button" data-testid="start-over-builder-button" onClick={onBackToLanding}>
              <House className="h-4 w-4" />
              Start over
            </button>
            <button className="primary-button" data-testid="open-preview-button" disabled={isSyncingPreview} onClick={onOpenPreview}>
              <Eye className="h-4 w-4" />
              {isSyncingPreview ? "Preparing..." : "Preview & export"}
            </button>
          </div>
        </div>
      </header>

      <div className="mx-auto grid w-full max-w-[1440px] gap-8 px-6 py-8 xl:grid-cols-[340px_minmax(0,1fr)]">
        <aside className="space-y-5 xl:sticky xl:top-24 xl:h-fit">
          <section className="panel p-5">
            <p className="section-heading">Dashboard settings</p>
            <div className="mt-5 space-y-4">
              <label className="block">
                <span className="mb-2 block text-sm font-semibold text-stone-700">Title</span>
                <input className="loom-input" onChange={(event) => onTitleChange(event.target.value)} value={title} />
              </label>
              <label className="block">
                <span className="mb-2 block text-sm font-semibold text-stone-700">Subtitle</span>
                <textarea
                  className="loom-textarea min-h-[96px]"
                  onChange={(event) => onSubtitleChange(event.target.value)}
                  value={subtitle}
                />
              </label>
            </div>
          </section>

          <section className="panel p-5">
            <div className="mb-4">
              <p className="section-heading">Content sections</p>
              <p className="mt-2 text-sm leading-6 text-stone-500">
                Use simple on/off switches and move sections up or down. No drag gestures required.
              </p>
            </div>

            <div className="mb-5">
              <label className="mb-2 block text-sm font-semibold text-stone-700">KPI cards</label>
              <input
                className="w-full"
                data-testid="metric-count-slider"
                max={maxMetricCount}
                min={Math.min(2, maxMetricCount)}
                onChange={(event) => onMetricCountChange(Number(event.target.value))}
                type="range"
                value={Math.min(metricCount, maxMetricCount)}
              />
              <p className="mt-2 text-sm text-stone-500">
                {Math.min(metricCount, maxMetricCount)} KPI cards shown in the lead section
              </p>
            </div>

            <button
              className="mb-5 flex w-full items-center justify-between rounded-2xl border border-stone-200 bg-[#f5f5f4]/82 px-4 py-3 text-left"
              data-testid="toggle-notes-button"
              onClick={() => onShowNotesChange(!showNotes)}
              type="button"
            >
              <div>
                <p className="text-sm font-semibold text-stone-900">Insight notes</p>
                <p className="text-sm text-stone-500">Keep the approved narrative notes in the final dashboard</p>
              </div>
              {showNotes ? (
                <ToggleRight className="h-8 w-8 text-[#c2410c]" />
              ) : (
                <ToggleLeft className="h-8 w-8 text-stone-400" />
              )}
            </button>

            <div className="space-y-3">
              {orderedSectionIds.map((sectionId, index) => (
                <SectionControlCard
                  active
                  canMoveDown={index < orderedSectionIds.length - 1}
                  canMoveUp={index > 0}
                  key={sectionId}
                  label={availableSections[sectionId]}
                  onMoveDown={() => moveSection(sectionId, "down")}
                  onMoveUp={() => moveSection(sectionId, "up")}
                  onToggle={() => onSectionOrderChange(sectionOrder.filter((item) => item !== sectionId))}
                />
              ))}
              {inactiveSectionIds.map((sectionId) => (
                <SectionControlCard
                  active={false}
                  canMoveDown={false}
                  canMoveUp={false}
                  key={sectionId}
                  label={availableSections[sectionId]}
                  onMoveDown={() => undefined}
                  onMoveUp={() => undefined}
                  onToggle={() => onSectionOrderChange([...sectionOrder, sectionId])}
                />
              ))}
            </div>
          </section>
        </aside>

        <main>
          <DashboardCanvas blueprint={blueprint} />
        </main>
      </div>
    </div>
  );
}

function SectionControlCard({
  label,
  active,
  canMoveUp,
  canMoveDown,
  onMoveUp,
  onMoveDown,
  onToggle,
}: {
  label: string;
  active: boolean;
  canMoveUp: boolean;
  canMoveDown: boolean;
  onMoveUp: () => void;
  onMoveDown: () => void;
  onToggle: () => void;
}) {
  return (
    <div
      className={cn(
        "rounded-2xl border px-4 py-4",
        active ? "border-[#fdba74] bg-[#fff7ed]/70" : "border-stone-200 bg-white/90 opacity-80"
      )}
      data-testid={`section-card-${label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <p className="text-sm font-semibold text-stone-900">{label}</p>
          <p className="mt-1 text-xs uppercase tracking-[0.18em] text-stone-500">{active ? "Included" : "Hidden"}</p>
        </div>
        <button
          className={cn(
            "rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em]",
            active ? "bg-[#292524] text-[#fafaf9]" : "border border-stone-200 bg-white text-stone-600"
          )}
          onClick={onToggle}
          type="button"
        >
          {active ? "On" : "Off"}
        </button>
      </div>

      <div className="mt-4 flex items-center gap-2">
        <button className="secondary-button px-3 py-2" disabled={!active || !canMoveUp} onClick={onMoveUp} type="button">
          <ArrowUp className="h-4 w-4" />
          Up
        </button>
        <button className="secondary-button px-3 py-2" disabled={!active || !canMoveDown} onClick={onMoveDown} type="button">
          <ArrowDown className="h-4 w-4" />
          Down
        </button>
      </div>
    </div>
  );
}

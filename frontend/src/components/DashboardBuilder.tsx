import {
  DndContext,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import { CSS } from "@dnd-kit/utilities";
import {
  SortableContext,
  arrayMove,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import {
  ArrowLeft,
  Eye,
  GripVertical,
  LayoutDashboard,
  ToggleLeft,
  ToggleRight,
} from "lucide-react";
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
  onBackToReview,
  onOpenPreview,
  isSyncingPreview,
  maxMetricCount,
}: DashboardBuilderProps) {
  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 6 } }));

  function handleDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }

    const oldIndex = sectionOrder.indexOf(String(active.id));
    const newIndex = sectionOrder.indexOf(String(over.id));
    if (oldIndex === -1 || newIndex === -1) {
      return;
    }
    onSectionOrderChange(arrayMove(sectionOrder, oldIndex, newIndex));
  }

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-20 border-b border-stone-200 bg-[#fafaf9]/90 backdrop-blur">
        <div className="mx-auto flex w-full max-w-[1440px] items-center justify-between px-6 py-4">
          <div className="flex items-center gap-4">
            <button className="ghost-button" onClick={onBackToReview}>
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
            <button className="primary-button" disabled={isSyncingPreview} onClick={onOpenPreview}>
              <Eye className="h-4 w-4" />
              {isSyncingPreview ? "Preparing..." : "Preview & export"}
            </button>
          </div>
        </div>
      </header>

      <div className="mx-auto grid w-full max-w-[1440px] gap-8 px-6 py-8 xl:grid-cols-[320px_minmax(0,1fr)]">
        <aside className="space-y-5 xl:sticky xl:top-24 xl:h-fit">
          <section className="panel p-5">
            <p className="section-heading">Dashboard settings</p>
            <div className="mt-5 space-y-4">
              <label className="block">
                <span className="mb-2 block text-sm font-semibold text-stone-700">Title</span>
                <input
                  className="loom-input"
                  onChange={(event) => onTitleChange(event.target.value)}
                  value={title}
                />
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
            <div className="mb-4 flex items-center justify-between">
              <p className="section-heading">Layout controls</p>
              <span className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">
                Drag to reorder
              </span>
            </div>

            <div className="mb-5">
              <label className="mb-2 block text-sm font-semibold text-stone-700">Metric cards</label>
              <input
                className="w-full"
                max={maxMetricCount}
                min={Math.min(2, maxMetricCount)}
                onChange={(event) => onMetricCountChange(Number(event.target.value))}
                type="range"
                value={Math.min(metricCount, maxMetricCount)}
              />
              <p className="mt-2 text-sm text-stone-500">
                {Math.min(metricCount, maxMetricCount)} cards in the overview section
              </p>
            </div>

            <button
              className="mb-5 flex w-full items-center justify-between rounded-2xl border border-stone-200 bg-[#f5f5f4]/82 px-4 py-3 text-left"
              onClick={() => onShowNotesChange(!showNotes)}
              type="button"
            >
              <div>
                <p className="text-sm font-semibold text-stone-900">Approved insight notes</p>
                <p className="text-sm text-stone-500">Keep context notes in the exported dashboard</p>
              </div>
              {showNotes ? (
                <ToggleRight className="h-8 w-8 text-[#c2410c]" />
              ) : (
                <ToggleLeft className="h-8 w-8 text-stone-400" />
              )}
            </button>

            <DndContext collisionDetection={closestCenter} onDragEnd={handleDragEnd} sensors={sensors}>
              <SortableContext items={sectionOrder} strategy={verticalListSortingStrategy}>
                <div className="space-y-3">
                  {Object.entries(availableSections).map(([sectionId, label]) => (
                    <SortableSectionItem
                      active={sectionOrder.includes(sectionId)}
                      key={sectionId}
                      label={label}
                      onToggle={() => {
                        if (sectionOrder.includes(sectionId)) {
                          onSectionOrderChange(sectionOrder.filter((item) => item !== sectionId));
                        } else {
                          onSectionOrderChange([...sectionOrder, sectionId]);
                        }
                      }}
                      sectionId={sectionId}
                    />
                  ))}
                </div>
              </SortableContext>
            </DndContext>
          </section>
        </aside>

        <main>
          <DashboardCanvas blueprint={blueprint} />
        </main>
      </div>
    </div>
  );
}

function SortableSectionItem({
  sectionId,
  label,
  active,
  onToggle,
}: {
  sectionId: string;
  label: string;
  active: boolean;
  onToggle: () => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id: sectionId, disabled: !active });

  return (
    <div
      className={cn(
        "flex items-center gap-3 rounded-2xl border px-4 py-3",
        active ? "border-[#fdba74] bg-[#fff7ed]/70" : "border-stone-200 bg-white/90 opacity-75"
      )}
      ref={setNodeRef}
      style={{
        transform: CSS.Transform.toString(transform),
        transition,
      }}
    >
      <button
        className={cn("ghost-button p-0", !active && "pointer-events-none opacity-30")}
        {...attributes}
        {...listeners}
        type="button"
      >
        <GripVertical className="h-4 w-4" />
      </button>
      <div className="flex-1">
        <p className="text-sm font-semibold text-stone-900">{label}</p>
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
  );
}

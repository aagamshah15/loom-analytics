import type {
  DashboardBlueprint,
  DashboardInsightCard,
  DashboardMetricCard,
  DashboardSection,
} from "../types";

type DraftSettings = {
  title: string;
  subtitle: string;
  sectionOrder: string[];
  metricCount: number;
  showNotes: boolean;
};

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function metricCardFromInsight(insight: DashboardInsightCard): DashboardMetricCard {
  return {
    id: insight.id,
    label: insight.metric_label,
    value: insight.metric_value,
    sub: insight.metric_sub,
    tone: insight.severity,
  };
}

export function rebuildDashboardBlueprint(
  baseBlueprint: DashboardBlueprint,
  settings: DraftSettings
): DashboardBlueprint {
  const blueprint = deepClone(baseBlueprint);
  blueprint.title = settings.title;
  blueprint.subtitle = settings.subtitle;
  blueprint.headline = {
    title: settings.title,
    subtitle: settings.subtitle,
  };
  blueprint.show_notes = settings.showNotes;
  blueprint.sections = settings.sectionOrder;
  blueprint.metric_cards = blueprint.approved_insights
    .slice(0, settings.metricCount)
    .map(metricCardFromInsight);

  const sectionsById = new Map<string, DashboardSection>(
    (blueprint.all_layout_sections ?? blueprint.layout_sections).map((section) => [section.id, section])
  );

  blueprint.layout_sections = settings.sectionOrder
    .map((sectionId) => sectionsById.get(sectionId))
    .filter((section): section is DashboardSection => Boolean(section))
    .filter((section) => {
      if (!settings.showNotes && (section.id === "notes" || section.id === "data_notes")) {
        return false;
      }
      return true;
    })
    .map((section) => {
      if (section.id !== "overview") {
        return section;
      }

      return {
        ...section,
        blocks: section.blocks.map((block) =>
          block.kind === "metric_grid"
            ? {
                ...block,
                cards: blueprint.metric_cards,
              }
            : block
        ),
      };
    });

  return blueprint;
}

export function downloadHtmlFile(filename: string, html: string) {
  const blob = new Blob([html], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

export type TemplateOption = {
  kind: string;
  label: string;
  description: string;
  implemented: boolean;
};

export type BusinessContext = {
  kind: string;
  display_name: string;
  confidence: number;
  analysis: Record<string, unknown>;
  sections: Record<string, string>;
  default_title: string;
  // Sections to switch on initially; null means "the ones the insights point at".
  default_sections?: string[] | null;
  // How many of the template's known columns the file supplied; null when it matched by content.
  match?: { found: number; expected: number } | null;
};

// How close a template came to matching: its closest accepted shape and the required fields it lacks.
export type DetectionCandidate = {
  kind: string;
  display_name: string;
  profile: string | null;
  matched: number;
  required: number;
  missing: string[];
};

export type InsightCandidate = {
  id: string;
  title: string;
  category: string;
  severity: "high" | "medium" | "low" | string;
  summary: string;
  detail: string;
  metric_label: string;
  metric_value: string;
  metric_sub: string;
  section: string;
  recommended?: boolean;
  score?: number;
};

export type ReviewBundle = {
  insights: InsightCandidate[];
  focus_tags: string[];
};

export type AnalyzeReport = {
  run_id: string;
  row_count: number;
  column_count: number;
  quality_report?: { score?: number };
  warnings?: string[];
  errors?: Array<{ message: string }>;
};

export type AnalyzeResponse = {
  report: AnalyzeReport;
  detected_template: BusinessContext | null;
  business_context: BusinessContext | null;
  review: ReviewBundle;
  detection?: { candidates: DetectionCandidate[] };
  template_options: TemplateOption[];
};

export type DashboardMetricCard = {
  id: string;
  label: string;
  value: string;
  sub: string;
  tone?: string;
};

export type DashboardInsightCard = {
  id: string;
  title: string;
  category: string;
  severity: string;
  summary: string;
  detail: string;
  metric_label: string;
  metric_value: string;
  metric_sub: string;
  section: string;
};

export type DashboardChartSeries = {
  name: string;
  values: number[];
  color?: string;
};

export type DashboardChart = {
  id: string;
  title: string;
  subtitle: string;
  type: "bar" | "line" | "area" | "pie";
  labels: string[];
  series: DashboardChartSeries[];
  format?: "currency" | "percent" | "number";
};

export type DashboardStatItem = {
  label: string;
  value: string;
  tone?: string;
};

export type DashboardBlock =
  | {
      id: string;
      kind: "metric_grid";
      cards: DashboardMetricCard[];
    }
  | {
      id: string;
      kind: "insight_grid";
      insights: DashboardInsightCard[];
    }
  | {
      id: string;
      kind: "chart";
      chart: DashboardChart;
    }
  | {
      id: string;
      kind: "stat_list";
      title: string;
      items: DashboardStatItem[];
    }
  | {
      id: string;
      kind: "note_list";
      insights: DashboardInsightCard[];
    };

export type DashboardSection = {
  id: string;
  title: string;
  description: string;
  blocks: DashboardBlock[];
};

export type DashboardBlueprint = {
  kind: string;
  title: string;
  subtitle: string;
  headline: { title: string; subtitle: string };
  approved_insights: DashboardInsightCard[];
  metric_cards: DashboardMetricCard[];
  sections: string[];
  layout_sections: DashboardSection[];
  all_layout_sections?: DashboardSection[];
  signals: Record<string, unknown>;
  summary: Record<string, unknown>;
  dataset: Record<string, unknown>;
  show_notes: boolean;
  focus_tags: string[];
};

export type DashboardResponse = {
  kind: string;
  title: string;
  html: string;
  height: number;
  payload: DashboardBlueprint;
  blueprint: DashboardBlueprint;
  download_name: string;
};

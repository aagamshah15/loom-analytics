import type { AnalyzeResponse, DashboardResponse, ReviewBundle } from "../types";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/+$/, "");

function apiUrl(path: string): string {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

export async function analyzeCsv(file: File, templateOverride?: string): Promise<AnalyzeResponse> {
  const formData = new FormData();
  formData.append("file", file);
  if (templateOverride) {
    formData.append("template_override", templateOverride);
  }

  const response = await fetch(apiUrl("/api/analyze"), {
    method: "POST",
    body: formData
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  return response.json();
}

export async function regenerateReview(
  kind: string,
  analysis: Record<string, unknown>,
  userPrompt: string
): Promise<ReviewBundle> {
  const response = await fetch(apiUrl("/api/review"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ kind, analysis, user_prompt: userPrompt })
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  return response.json();
}

export async function buildDashboard(payload: {
  kind: string;
  analysis: Record<string, unknown>;
  approvedInsightIds: string[];
  userPrompt: string;
  settings: {
    title: string;
    subtitle: string;
    includedSections: string[];
    metricCount: number;
    showNotes: boolean;
  };
}): Promise<DashboardResponse> {
  const response = await fetch(apiUrl("/api/build-dashboard"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      kind: payload.kind,
      analysis: payload.analysis,
      approved_insight_ids: payload.approvedInsightIds,
      user_prompt: payload.userPrompt,
      settings: {
        title: payload.settings.title,
        subtitle: payload.settings.subtitle,
        included_sections: payload.settings.includedSections,
        metric_count: payload.settings.metricCount,
        show_notes: payload.settings.showNotes
      }
    })
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  return response.json();
}

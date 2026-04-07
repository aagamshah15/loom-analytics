import { AlertTriangle, Database, ShieldCheck, TableProperties } from "lucide-react";
import type { AnalyzeReport } from "../types";

type RunSummaryProps = {
  report: AnalyzeReport;
  inputName: string;
};

export function RunSummary({ report, inputName }: RunSummaryProps) {
  const cards = [
    {
      label: "Quality score",
      value: report.quality_report?.score != null ? String(report.quality_report.score) : "n/a",
      icon: <ShieldCheck className="h-4 w-4 text-emerald-500" />,
    },
    {
      label: "Rows",
      value: report.row_count.toLocaleString(),
      icon: <Database className="h-4 w-4 text-[#c2410c]" />,
    },
    {
      label: "Columns",
      value: String(report.column_count),
      icon: <TableProperties className="h-4 w-4 text-stone-500" />,
    },
    {
      label: "Warnings",
      value: String(report.warnings?.length ?? 0),
      icon: <AlertTriangle className="h-4 w-4 text-amber-500" />,
    },
  ];

  return (
    <div className="panel p-5">
      <div className="mb-5 flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="section-heading">Run Summary</p>
          <h3 className="editorial-heading mt-2 text-2xl font-bold">{inputName}</h3>
        </div>
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map((card) => (
          <div className="metric-card flex items-center justify-between" key={card.label}>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{card.label}</p>
              <p className="mt-2 text-3xl font-bold tracking-tight text-stone-900">{card.value}</p>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white p-2">{card.icon}</div>
          </div>
        ))}
      </div>
      {report.warnings?.length ? (
        <div className="mt-4 rounded-2xl border border-amber-200 bg-[#fffbeb] px-4 py-3 text-sm text-amber-800">
          {report.warnings[0]}
        </div>
      ) : null}
    </div>
  );
}

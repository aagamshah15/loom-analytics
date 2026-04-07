import type { ReactNode } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { AlertCircle, CheckCircle2, Info, TrendingUp } from "lucide-react";
import type {
  DashboardBlock,
  DashboardBlueprint,
  DashboardChart,
  DashboardInsightCard,
  DashboardMetricCard,
  DashboardStatItem,
} from "../../types";
import { cn } from "../../lib/cn";

const CHART_COLORS = ["#c2410c", "#292524", "#166534", "#b45309", "#9a3412", "#0f766e"];
const GRID_STROKE = "#d6d3d1";
const AXIS_STROKE = "#78716c";
const PIE_COLORS = CHART_COLORS;

type DashboardCanvasProps = {
  blueprint: DashboardBlueprint;
  mode?: "builder" | "preview";
};

export function DashboardCanvas({ blueprint, mode = "builder" }: DashboardCanvasProps) {
  return (
    <div className={cn("space-y-8", mode === "preview" ? "mx-auto max-w-7xl px-6 py-8" : "")}>
      <header className="panel overflow-hidden p-8">
        <div className="grid gap-6 lg:grid-cols-[1.5fr_0.7fr]">
          <div>
            <p className="section-heading">Dashboard narrative</p>
            <h1 className="editorial-heading mt-3 text-4xl font-bold md:text-5xl">
              {blueprint.title}
            </h1>
            <p className="mt-4 max-w-3xl text-base leading-7 text-stone-600 md:text-lg">
              {blueprint.subtitle}
            </p>
            {blueprint.focus_tags.length ? (
              <div className="mt-6 flex flex-wrap gap-2">
                {blueprint.focus_tags.map((tag) => (
                  <span className="loom-chip" key={tag}>
                    {tag}
                  </span>
                ))}
              </div>
            ) : null}
          </div>

          <div className="grid gap-3 self-start rounded-2xl border border-stone-200 bg-[#f5f5f4]/82 p-5">
            <SummaryDatum
              label="Approved insights"
              value={String(blueprint.approved_insights.length)}
              icon={<CheckCircle2 className="h-4 w-4 text-emerald-500" />}
            />
            <SummaryDatum
              label="Sections"
              value={String(blueprint.layout_sections.length)}
              icon={<TrendingUp className="h-4 w-4 text-[#c2410c]" />}
            />
            <SummaryDatum
              label="Dataset rows"
              value={formatMaybeNumber(blueprint.dataset.row_count)}
              icon={<Info className="h-4 w-4 text-stone-500" />}
            />
          </div>
        </div>
      </header>

      {blueprint.layout_sections.map((section) => (
        <section className="panel p-6 md:p-8" key={section.id}>
          <div className="mb-6">
            <p className="section-heading">{section.title}</p>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-stone-600 md:text-base">
              {section.description}
            </p>
          </div>

          <div className="space-y-5">
            {section.blocks.map((block) => (
              <BlockRenderer block={block} key={block.id} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

function SummaryDatum({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: ReactNode;
}) {
  return (
    <div className="flex items-center justify-between rounded-2xl border border-stone-200 bg-white px-4 py-3">
      <div>
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{label}</p>
        <p className="mt-1 text-lg font-bold text-stone-900">{value}</p>
      </div>
      <div className="rounded-xl border border-stone-200 bg-[#fafaf9] p-2">{icon}</div>
    </div>
  );
}

function BlockRenderer({ block }: { block: DashboardBlock }) {
  if (block.kind === "metric_grid") {
    return <MetricGrid cards={block.cards} />;
  }
  if (block.kind === "insight_grid") {
    return <InsightGrid insights={block.insights} />;
  }
  if (block.kind === "chart") {
    return <ChartBlock chart={block.chart} />;
  }
  if (block.kind === "stat_list") {
    return <StatList title={block.title} items={block.items} />;
  }
  return <NoteList insights={block.insights} />;
}

function MetricGrid({ cards }: { cards: DashboardMetricCard[] }) {
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      {cards.map((card) => (
        <div className="metric-card" key={card.id}>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{card.label}</p>
          <p className="mt-3 text-2xl font-bold tracking-tight text-stone-900">{card.value}</p>
          <p className="mt-2 text-sm text-stone-500">{card.sub}</p>
        </div>
      ))}
    </div>
  );
}

function InsightGrid({ insights }: { insights: DashboardInsightCard[] }) {
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      {insights.map((insight) => (
        <article
          className={cn(
            "rounded-2xl border p-5",
            insight.severity === "high" && "border-[#fdba74] bg-[#fff7ed]/80",
            insight.severity === "medium" && "border-amber-200 bg-[#fffbeb]/90",
            insight.severity === "low" && "border-emerald-200 bg-emerald-50/80"
          )}
          key={insight.id}
        >
          <div className="mb-4 flex items-center justify-between gap-3">
            <span className="rounded-full border border-white/80 bg-white/85 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-stone-700">
              {insight.category}
            </span>
            <SeverityIcon severity={insight.severity} />
          </div>
          <h3 className="font-display text-2xl font-bold text-stone-900">{insight.title}</h3>
          <p className="mt-2 text-sm leading-6 text-stone-600">{insight.summary}</p>
          <div className="mt-5 rounded-xl border border-white/80 bg-white/90 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">{insight.metric_label}</p>
            <p className="mt-2 text-xl font-bold text-stone-900">{insight.metric_value}</p>
            <p className="mt-1 text-sm text-stone-500">{insight.metric_sub}</p>
          </div>
        </article>
      ))}
    </div>
  );
}

function NoteList({ insights }: { insights: DashboardInsightCard[] }) {
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      {insights.map((insight) => (
        <article className="rounded-2xl border border-stone-200 bg-[#f5f5f4]/82 p-5" key={insight.id}>
          <div className="mb-3 flex items-center gap-2">
            <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold uppercase tracking-wide text-stone-600">
              {insight.category}
            </span>
          </div>
          <h4 className="font-display text-xl font-bold text-stone-900">{insight.title}</h4>
          <p className="mt-2 text-sm leading-6 text-stone-600">{insight.detail}</p>
        </article>
      ))}
    </div>
  );
}

function StatList({ title, items }: { title: string; items: DashboardStatItem[] }) {
  return (
    <div className="panel-soft p-5">
      <div className="mb-4 flex items-center justify-between">
        <h4 className="text-sm font-semibold uppercase tracking-[0.18em] text-stone-500">{title}</h4>
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {items.map((item) => (
          <div className="rounded-2xl border border-stone-200 bg-white p-4" key={`${item.label}-${item.value}`}>
            <p className="text-sm text-stone-500">{item.label}</p>
            <p
              className={cn(
                "mt-2 text-xl font-bold tracking-tight text-stone-900",
                item.tone === "danger" && "text-rose-600",
                item.tone === "positive" && "text-emerald-600",
                item.tone === "warning" && "text-amber-600"
              )}
            >
              {item.value}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}

function ChartBlock({ chart }: { chart: DashboardChart }) {
  const data = chart.labels.map((label, index) => {
    const row: Record<string, string | number> = { label };
    for (const series of chart.series) {
      row[series.name] = series.values[index] ?? 0;
    }
    return row;
  });

  return (
    <div className="panel-soft p-5">
      <div className="mb-5">
        <h4 className="font-display text-2xl font-bold text-stone-900">{chart.title}</h4>
        <p className="mt-2 text-sm text-stone-500">{chart.subtitle}</p>
      </div>

      <div className="h-72 w-full">
        <ResponsiveContainer width="100%" height="100%">
          {chart.type === "pie" ? (
            <PieChart>
              <Tooltip formatter={(value: number) => formatChartValue(value, chart.format)} />
              <Legend />
              <Pie
                data={data.map((row, index) => ({
                  name: String(row.label),
                  value: Number(row[chart.series[0]?.name ?? "value"] ?? 0),
                  fill: PIE_COLORS[index % PIE_COLORS.length],
                }))}
                cx="50%"
                cy="50%"
                dataKey="value"
                innerRadius={52}
                outerRadius={96}
                paddingAngle={3}
              >
                {data.map((_, index) => (
                  <Cell fill={PIE_COLORS[index % PIE_COLORS.length]} key={`${chart.id}-${index}`} />
                ))}
              </Pie>
            </PieChart>
          ) : chart.type === "line" ? (
            <LineChart data={data}>
              <CartesianGrid stroke={GRID_STROKE} strokeDasharray="3 3" />
              <XAxis dataKey="label" stroke={AXIS_STROKE} tickLine={false} axisLine={false} />
              <YAxis
                stroke={AXIS_STROKE}
                tickFormatter={(value: number) => axisTickFormatter(value, chart.format)}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip formatter={(value: number) => formatChartValue(value, chart.format)} />
              <Legend />
              {chart.series.map((series, index) => (
                <Line
                  dataKey={series.name}
                  dot={false}
                  key={series.name}
                  stroke={series.color ?? PIE_COLORS[index % PIE_COLORS.length]}
                  strokeWidth={3}
                  type="monotone"
                />
              ))}
            </LineChart>
          ) : chart.type === "area" ? (
            <AreaChart data={data}>
              <CartesianGrid stroke={GRID_STROKE} strokeDasharray="3 3" />
              <XAxis dataKey="label" stroke={AXIS_STROKE} tickLine={false} axisLine={false} />
              <YAxis
                stroke={AXIS_STROKE}
                tickFormatter={(value: number) => axisTickFormatter(value, chart.format)}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip formatter={(value: number) => formatChartValue(value, chart.format)} />
              <Legend />
              {chart.series.map((series, index) => (
                <Area
                  dataKey={series.name}
                  fill={series.color ?? PIE_COLORS[index % PIE_COLORS.length]}
                  fillOpacity={0.15}
                  key={series.name}
                  stroke={series.color ?? PIE_COLORS[index % PIE_COLORS.length]}
                  strokeWidth={3}
                  type="monotone"
                />
              ))}
            </AreaChart>
          ) : (
            <BarChart data={data}>
              <CartesianGrid stroke={GRID_STROKE} strokeDasharray="3 3" />
              <XAxis dataKey="label" stroke={AXIS_STROKE} tickLine={false} axisLine={false} />
              <YAxis
                stroke={AXIS_STROKE}
                tickFormatter={(value: number) => axisTickFormatter(value, chart.format)}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip formatter={(value: number) => formatChartValue(value, chart.format)} />
              <Legend />
              {chart.series.map((series, index) => (
                <Bar
                  dataKey={series.name}
                  fill={series.color ?? PIE_COLORS[index % PIE_COLORS.length]}
                  key={series.name}
                  radius={[8, 8, 0, 0]}
                />
              ))}
            </BarChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === "high") {
    return <AlertCircle className="h-5 w-5 text-rose-500" />;
  }
  if (severity === "medium") {
    return <Info className="h-5 w-5 text-amber-500" />;
  }
  return <CheckCircle2 className="h-5 w-5 text-emerald-500" />;
}

function axisTickFormatter(value: number, format?: DashboardChart["format"]) {
  if (format === "currency") {
    return `$${Number(value).toFixed(0)}`;
  }
  if (format === "percent") {
    return `${Number(value).toFixed(0)}%`;
  }
  return `${Number(value).toFixed(0)}`;
}

function formatChartValue(value: number, format?: DashboardChart["format"]) {
  if (format === "currency") {
    return `$${Number(value).toLocaleString(undefined, {
      maximumFractionDigits: 2,
    })}`;
  }
  if (format === "percent") {
    return `${Number(value).toFixed(2)}%`;
  }
  return Number(value).toLocaleString();
}

function formatMaybeNumber(value: unknown) {
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  return value == null ? "n/a" : String(value);
}

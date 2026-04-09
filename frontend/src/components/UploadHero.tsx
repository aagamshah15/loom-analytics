import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ArrowRight, BookOpen, FileSpreadsheet, GitMerge, HeartPulse, LayoutTemplate, ShoppingBag, Sparkles, TrendingUp, Upload, BriefcaseBusiness, BarChart3, CircleHelp, ExternalLink } from "lucide-react";
import type { TemplateOption } from "../types";
import { cn } from "../lib/cn";

type UploadHeroProps = {
  onFileSelected: (file: File) => void;
  onAnalyze: () => void;
  file: File | null;
  templateOptions: TemplateOption[];
  disabled?: boolean;
};

type LandingSection = "analyze" | "templates" | "docs";

const NAV_ITEMS: Array<{ id: LandingSection; label: string }> = [
  { id: "analyze", label: "Analyze" },
  { id: "templates", label: "Templates" },
  { id: "docs", label: "Docs" },
];

const TEMPLATE_BEST_FOR: Record<string, string> = {
  financial_timeseries: "Best for OHLCV market histories, equities, and long-horizon price series.",
  ecommerce_orders: "Best for orders, returns, discounting, channel quality, and customer behavior.",
  healthcare_medical: "Best for patient outcomes, adherence, payer mix, and care delivery data.",
  hr_workforce: "Best for attrition, pay equity, engagement, training, and workforce risk.",
  marketing_campaign: "Best for spend, clicks, conversion, and ROAS-style campaign tables.",
  survey_sentiment: "Best for rating-based feedback and response-level sentiment exports.",
  web_app_analytics: "Best for sessions, funnels, device behavior, and event analytics.",
  generic: "Best when you want a safe fallback while Loom learns the right business shape.",
};

const TEMPLATE_ICON_MAP = {
  financial_timeseries: TrendingUp,
  ecommerce_orders: ShoppingBag,
  healthcare_medical: HeartPulse,
  hr_workforce: BriefcaseBusiness,
  generic: BarChart3,
  marketing_campaign: Sparkles,
  survey_sentiment: CircleHelp,
  web_app_analytics: LayoutTemplate,
};

export function UploadHero({ onFileSelected, onAnalyze, file, templateOptions, disabled = false }: UploadHeroProps) {
  const analyzeRef = useRef<HTMLElement | null>(null);
  const templatesRef = useRef<HTMLElement | null>(null);
  const docsRef = useRef<HTMLElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [activeSection, setActiveSection] = useState<LandingSection>("analyze");

  const implementedTemplates = useMemo(
    () => templateOptions.filter((template) => template.implemented && template.kind !== "generic"),
    [templateOptions]
  );
  const comingSoonTemplates = useMemo(
    () => templateOptions.filter((template) => !template.implemented),
    [templateOptions]
  );

  useEffect(() => {
    const sections = [
      { id: "analyze" as const, element: analyzeRef.current },
      { id: "templates" as const, element: templatesRef.current },
      { id: "docs" as const, element: docsRef.current },
    ].filter((section): section is { id: LandingSection; element: HTMLElement } => Boolean(section.element));

    const updateActiveSection = () => {
      const topOffset = 140;
      let nextActive: LandingSection = "analyze";

      for (const section of sections) {
        const sectionTop = section.element.getBoundingClientRect().top;
        if (sectionTop <= topOffset) {
          nextActive = section.id;
        }
      }

      setActiveSection(nextActive);
    };

    updateActiveSection();
    window.addEventListener("scroll", updateActiveSection, { passive: true });
    window.addEventListener("resize", updateActiveSection);

    return () => {
      window.removeEventListener("scroll", updateActiveSection);
      window.removeEventListener("resize", updateActiveSection);
    };
  }, []);

  function scrollToSection(section: LandingSection) {
    const target =
      section === "analyze" ? analyzeRef.current : section === "templates" ? templatesRef.current : docsRef.current;

    setActiveSection(section);
    target?.scrollIntoView({ behavior: "smooth", block: "start" });

    if (section === "analyze") {
      window.setTimeout(() => {
        fileInputRef.current?.focus();
      }, 350);
    }
  }

  return (
    <div className="min-h-screen" data-testid="landing-screen">
      <nav className="sticky top-0 z-20 border-b border-stone-200/90 bg-[#fafaf9]/85 backdrop-blur">
        <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-4 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-[#c2410c] p-2 text-white shadow-sm">
              <GitMerge className="h-5 w-5" />
            </div>
            <div>
              <p className="font-display text-2xl font-bold tracking-tight text-stone-900">Loom</p>
              <p className="text-xs uppercase tracking-[0.24em] text-stone-500">From CSV to dashboard</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-stone-600" data-testid="landing-nav">
            {NAV_ITEMS.map((item) => (
              <button
                className={cn(
                  "rounded-full px-4 py-2 transition",
                  activeSection === item.id ? "bg-white text-stone-900 shadow-sm" : "text-stone-600 hover:bg-white/80 hover:text-stone-900"
                )}
                data-testid={`landing-nav-${item.id}`}
                key={item.id}
                onClick={() => scrollToSection(item.id)}
                type="button"
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      </nav>

      <div className="mx-auto grid min-h-[calc(100vh-81px)] w-full max-w-7xl gap-10 px-6 py-16 lg:grid-cols-[1.1fr_0.9fr] lg:items-center">
        <section className="relative">
          <div className="absolute -left-8 top-4 hidden h-56 w-56 rounded-full bg-orange-100/60 blur-3xl lg:block" />
          <div className="relative">
            <span className="kicker">
              <GitMerge className="h-3.5 w-3.5 text-[#c2410c]" />
              Weave your data into a clear story
            </span>
          </div>
          <h1 className="editorial-heading mt-6 max-w-4xl text-5xl font-bold md:text-6xl xl:text-7xl">
            From raw threads to
            <span className="block italic text-[#c2410c]">clear narratives.</span>
          </h1>
          <p className="mt-6 max-w-2xl text-lg leading-8 text-stone-600">
            Upload a dataset, review the non-obvious findings, and shape the final dashboard only after the story feels
            right. Loom keeps the workflow operational while making the output feel editorial and business-ready.
          </p>

          <div className="mt-10 grid gap-4 md:grid-cols-3">
            <HeroStep
              icon={<Upload className="h-5 w-5 text-[#c2410c]" />}
              step="Upload"
              blurb="Bring in any CSV up to 50MB with no template setup required."
            />
            <HeroStep
              icon={<Sparkles className="h-5 w-5 text-[#c2410c]" />}
              step="Review"
              blurb="Approve only the findings that deserve space in the final story."
            />
            <HeroStep
              icon={<FileSpreadsheet className="h-5 w-5 text-[#c2410c]" />}
              step="Build"
              blurb="Compose the dashboard, preview it, and export static HTML."
            />
          </div>
        </section>

        <section
          className="panel relative overflow-hidden p-6 scroll-mt-28 md:p-8"
          data-testid="analyze-section"
          id="analyze"
          ref={analyzeRef}
          tabIndex={-1}
        >
          <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-orange-300/80 to-transparent" />
          <label className="group block cursor-pointer rounded-[28px] border-2 border-dashed border-stone-300 bg-[#f5f5f4]/85 p-8 transition hover:border-[#c2410c]/40 hover:bg-[#fff7ed]/60">
            <input
              accept=".csv"
              className="sr-only"
              data-testid="csv-file-input"
              ref={fileInputRef}
              type="file"
              onChange={(event) => {
                const nextFile = event.target.files?.[0];
                if (nextFile) {
                  onFileSelected(nextFile);
                }
              }}
            />
            <div className="mx-auto flex max-w-md flex-col items-center text-center">
              <div className="rounded-full bg-white p-5 text-[#c2410c] shadow-sm transition group-hover:-translate-y-1">
                <Upload className="h-10 w-10" />
              </div>
              <h2 className="font-display mt-6 text-3xl font-bold tracking-tight text-stone-900">
                {file ? file.name : "Drag and drop your CSV"}
              </h2>
              <p className="mt-3 text-sm leading-6 text-stone-500">
                {file
                  ? `${(file.size / 1024 / 1024).toFixed(2)} MB selected`
                  : "CSV only. UTF-8 works best for this release."}
              </p>
              <div className="mt-6 rounded-full border border-stone-300 bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-stone-700">
                Browse files
              </div>
            </div>
          </label>

          <div className="mt-6 flex flex-col gap-4">
            <button
              className="primary-button w-full py-3 text-base"
              data-testid="analyze-csv-button"
              disabled={!file || disabled}
              onClick={onAnalyze}
            >
              Analyze CSV
              <ArrowRight className="h-4 w-4" />
            </button>
            <p className="text-center text-sm text-stone-500">
              We validate, clean, detect the best business template, and prepare the first insight review automatically.
            </p>
          </div>
        </section>
      </div>

      <div className="mx-auto w-full max-w-7xl space-y-10 px-6 pb-20">
        <section
          className="panel p-8 scroll-mt-28 md:p-10"
          data-testid="templates-section"
          id="templates"
          ref={templatesRef}
        >
          <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="section-heading">Template Catalog</p>
              <h2 className="editorial-heading mt-3 text-3xl font-bold md:text-4xl">Specialized stories, matched to the right data shape</h2>
              <p className="mt-4 max-w-3xl text-base leading-7 text-stone-600">
                Loom looks for strong business context before it starts making claims. These are the templates currently available in the product.
              </p>
            </div>
            <div className="kicker">
              <LayoutTemplate className="h-3.5 w-3.5 text-[#c2410c]" />
              {implementedTemplates.length} ready now
            </div>
          </div>

          <div className="mt-8 grid gap-4 lg:grid-cols-2">
            {implementedTemplates.map((template) => (
              <TemplateShowcaseCard key={template.kind} template={template} />
            ))}
          </div>

          {comingSoonTemplates.length ? (
            <>
              <div className="mt-10 flex items-center gap-3">
                <span className="section-heading !text-stone-500">Coming Soon</span>
              </div>
              <div className="mt-4 grid gap-4 lg:grid-cols-3">
                {comingSoonTemplates.map((template) => (
                  <TemplateShowcaseCard key={template.kind} muted template={template} />
                ))}
              </div>
            </>
          ) : null}
        </section>

        <section
          className="panel p-8 scroll-mt-28 md:p-10"
          data-testid="docs-section"
          id="docs"
          ref={docsRef}
        >
          <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="section-heading">Quick Start</p>
              <h2 className="editorial-heading mt-3 text-3xl font-bold md:text-4xl">What Loom expects, and what it does next</h2>
              <p className="mt-4 max-w-3xl text-base leading-7 text-stone-600">
                The product is intentionally linear: upload, review the hidden story, then build only what deserves to be shown.
              </p>
            </div>
            <div className="kicker">
              <BookOpen className="h-3.5 w-3.5 text-[#c2410c]" />
              End-user guide
            </div>
          </div>

          <div className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <DocsStepCard
              index="01"
              title="Upload"
              body="Bring in a CSV file. UTF-8 works best, and Loom currently expects tabular business data rather than free-form exports."
            />
            <DocsStepCard
              index="02"
              title="Detect"
              body="Loom checks the schema and tries to match a business template. If the shape is partial or unfamiliar, it falls back safely."
            />
            <DocsStepCard
              index="03"
              title="Review"
              body="Approve or reject the findings one by one, and use the prompt box to shift emphasis before anything is built."
            />
            <DocsStepCard
              index="04"
              title="Build"
              body="Only approved insights shape the dashboard. Then you can preview it, adjust sections, and export static HTML."
            />
          </div>

          <div className="mt-8 grid gap-4 lg:grid-cols-2">
            <HelpCard
              title="Why didn't my file match a specialized template?"
              body="Usually the CSV is missing a few key columns, uses unfamiliar names, or mixes multiple business entities in one table. Loom will stay generic rather than forcing the wrong story."
            />
            <HelpCard
              title="What if I upload the wrong file type?"
              body="Loom rejects non-CSV uploads right away. If the file opens in Sheets or Excel as a real table and exports cleanly to CSV, it should be a good candidate."
            />
            <HelpCard
              title="What does the prompt box actually do?"
              body="It does a deterministic re-ranking pass over the candidate insights. It changes emphasis and recommendation order, but it does not invent new unsupported analysis."
            />
            <HelpCard
              title="Why am I seeing the generic fallback?"
              body="Generic mode is the safe path when the dataset is incomplete, ambiguous, or not yet covered by a specialized template. It is a guardrail, not a failure state."
            />
          </div>

          <div className="mt-8 flex flex-wrap items-center gap-3 border-t border-stone-200 pt-6">
            <a
              className="secondary-button"
              href="https://github.com/aagamshah15/loom-analytics#readme"
              rel="noreferrer"
              target="_blank"
            >
              <ExternalLink className="h-4 w-4" />
              View full README
            </a>
          </div>
        </section>
      </div>
    </div>
  );
}

function TemplateShowcaseCard({ template, muted = false }: { template: TemplateOption; muted?: boolean }) {
  const Icon = TEMPLATE_ICON_MAP[template.kind as keyof typeof TEMPLATE_ICON_MAP] ?? LayoutTemplate;

  return (
    <article
      className={cn(
        "rounded-[28px] border p-6 transition",
        muted ? "border-stone-200 bg-[#f5f5f4]/70 opacity-75" : "border-stone-200 bg-white shadow-sm"
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className={cn("rounded-2xl p-3", muted ? "bg-stone-200 text-stone-500" : "bg-[#fff7ed] text-[#c2410c]")}>
          <Icon className="h-5 w-5" />
        </div>
        <span
          className={cn(
            "rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em]",
            template.implemented ? "bg-emerald-50 text-emerald-700" : "bg-stone-200 text-stone-600"
          )}
        >
          {template.implemented ? "Ready" : "Coming soon"}
        </span>
      </div>

      <h3 className="font-display mt-5 text-2xl font-bold text-stone-900">{template.label}</h3>
      <p className="mt-3 text-sm leading-6 text-stone-600">{template.description}</p>
      <p className="mt-4 text-sm font-medium leading-6 text-stone-500">
        {TEMPLATE_BEST_FOR[template.kind] ?? "Best for structured tabular business data."}
      </p>
    </article>
  );
}

function DocsStepCard({ index, title, body }: { index: string; title: string; body: string }) {
  return (
    <article className="panel-soft p-5">
      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[#c2410c]">{index}</p>
      <h3 className="mt-3 font-display text-2xl font-bold text-stone-900">{title}</h3>
      <p className="mt-3 text-sm leading-6 text-stone-600">{body}</p>
    </article>
  );
}

function HelpCard({ title, body }: { title: string; body: string }) {
  return (
    <article className="rounded-[28px] border border-stone-200 bg-white p-6 shadow-sm">
      <h3 className="font-display text-2xl font-bold text-stone-900">{title}</h3>
      <p className="mt-3 text-sm leading-7 text-stone-600">{body}</p>
    </article>
  );
}

function HeroStep({
  icon,
  step,
  blurb,
}: {
  icon: ReactNode;
  step: string;
  blurb: string;
}) {
  return (
    <div className="panel-soft p-5">
      <div className="mb-3 inline-flex rounded-xl border border-stone-200 bg-white p-3">{icon}</div>
      <p className="text-sm font-bold uppercase tracking-[0.18em] text-stone-500">{step}</p>
      <p className="mt-2 text-sm leading-6 text-stone-600">{blurb}</p>
    </div>
  );
}

import type { ReactNode } from "react";
import { ArrowRight, FileSpreadsheet, GitMerge, Sparkles, Upload } from "lucide-react";

type UploadHeroProps = {
  onFileSelected: (file: File) => void;
  onAnalyze: () => void;
  file: File | null;
  disabled?: boolean;
};

export function UploadHero({ onFileSelected, onAnalyze, file, disabled = false }: UploadHeroProps) {
  return (
    <div className="min-h-screen">
      <nav className="border-b border-stone-200/90 bg-[#fafaf9]/85 backdrop-blur">
        <div className="mx-auto flex w-full max-w-7xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-[#c2410c] p-2 text-white shadow-sm">
              <GitMerge className="h-5 w-5" />
            </div>
            <div>
              <p className="font-display text-2xl font-bold tracking-tight text-stone-900">Loom</p>
              <p className="text-xs uppercase tracking-[0.24em] text-stone-500">From CSV to dashboard</p>
            </div>
          </div>
          <div className="hidden items-center gap-8 text-sm font-medium text-stone-600 md:flex">
            <span className="text-stone-900">Analyze</span>
            <span>Templates</span>
            <span>Docs</span>
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

        <section className="panel relative overflow-hidden p-6 md:p-8">
          <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-orange-300/80 to-transparent" />
          <label className="group block cursor-pointer rounded-[28px] border-2 border-dashed border-stone-300 bg-[#f5f5f4]/85 p-8 transition hover:border-[#c2410c]/40 hover:bg-[#fff7ed]/60">
            <input
              accept=".csv"
              className="sr-only"
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
            <button className="primary-button w-full py-3 text-base" disabled={!file || disabled} onClick={onAnalyze}>
              Analyze CSV
              <ArrowRight className="h-4 w-4" />
            </button>
            <p className="text-center text-sm text-stone-500">
              We validate, clean, detect the best business template, and prepare the first insight review automatically.
            </p>
          </div>
        </section>
      </div>
    </div>
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

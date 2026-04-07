import { GitMerge } from "lucide-react";

type LoadingOverlayProps = {
  progress: number;
  stage: string;
};

export function LoadingOverlay({ progress, stage }: LoadingOverlayProps) {
  return (
    <div className="mx-auto flex min-h-screen w-full max-w-3xl flex-col items-center justify-center px-6">
      <div className="panel w-full max-w-2xl p-8 text-center md:p-10">
        <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-[#fff7ed] text-[#c2410c]">
          <GitMerge className="h-8 w-8 animate-pulse" />
        </div>
        <p className="section-heading mt-8">Analysis in progress</p>
        <h2 className="editorial-heading mt-4 text-3xl font-bold">{stage}</h2>
        <p className="mt-4 text-base leading-7 text-stone-600">
          Loom is cleaning the dataset, confirming the right template, and drafting an initial set of findings you can
          refine before building the dashboard.
        </p>

        <div className="mt-8 overflow-hidden rounded-full bg-stone-200">
          <div className="h-3 rounded-full bg-[#c2410c] transition-all duration-300" style={{ width: `${progress}%` }} />
        </div>
        <div className="mt-3 flex items-center justify-between text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">
          <span>Processing</span>
          <span>{progress}%</span>
        </div>
      </div>
    </div>
  );
}

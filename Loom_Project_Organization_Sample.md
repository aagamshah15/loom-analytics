# Loom Project Organization Sample

## Purpose
This is a sample of how I keep a project organized and on track. It uses my current work on **Loom**, a CSV-to-dashboard product, as the example.

Loom takes raw CSV files, detects the right business template, surfaces non-obvious insights, and turns approved insights into a shareable dashboard.

## 1. Project Snapshot

**Project:** Loom  
**Stage:** MVP build + stability hardening  
**Goal:** Make the workflow feel simple for end users while keeping the backend reliable across many different CSV shapes.

### Current product flow
1. Upload CSV
2. Detect business template
3. Review and approve insights
4. Build dashboard
5. Preview and export

### Current stack
- React + Vite frontend
- FastAPI backend
- Python data pipeline
- Netlify frontend hosting
- Render backend hosting

## 2. How I Organize the Work

I keep the project organized in **four parallel lanes** so feature work does not bury reliability work.

| Lane | Focus | Current Status | How I Track It |
|---|---|---|---|
| Product UX | Landing flow, review flow, builder, preview | In progress | UI checklist + live app pass |
| Template Logic | Financial, e-commerce, healthcare, HR, marketing, survey, web analytics | In progress | Template matrix + fixture tests |
| Reliability | Stress testing, edge cases, fallback behavior, regression tests | In progress | Automated test suite + manual checklist |
| Deployment | Netlify, Render, environment variables, live smoke checks | MVP live | Hosted smoke test |

## 3. Timeline / Delivery Plan

| Phase | Outcome | Status |
|---|---|---|
| Phase 1: Core pipeline foundation | CSV ingestion, validation, cleaning, analysis pipeline working | Complete |
| Phase 2: Prototype workflow | Streamlit prototype proved the end-to-end idea | Complete |
| Phase 3: Product UI rebuild | React/Vite app with upload, review, builder, preview flow | Complete for MVP |
| Phase 4: Specialized templates | Business-template logic added and expanded | In progress |
| Phase 5: Stress testing | Fixture system, browser tests, hosted smoke checks | In progress |
| Phase 6: Submission readiness | Polishing reliability, documentation, and reviewer experience | Current focus |

## 4. Weekly Operating Checklist

This is the checklist style I use to keep momentum without losing the thread.

### A. Build
- Ship one visible improvement to the product flow
- Keep the React app and backend contract aligned
- Update the README or docs when the workflow changes

### B. Verify
- Run backend tests
- Run frontend build
- Test at least one real CSV locally
- Re-test one previously broken CSV to guard against regressions

### C. Expand
- Add or improve one template or one schema variant
- Add a fixture for any dataset that exposed a bug
- Add a regression test for every bug that gets fixed

### D. Deploy
- Push only after local verification passes
- Confirm Netlify still points to the correct Render backend
- Smoke test the live app after deploy

## 5. Current Workstream Tracker

| Workstream | What “done” means | Status | Next step |
|---|---|---|---|
| Landing + navigation | Clear entry point, useful nav, better first impression | Done for MVP | Minor polish only |
| Insight review flow | Approve/reject insights, prompt refresh, navigation clarity | In progress | Keep simplifying controls |
| Builder UX | Sections are understandable and reorderable | In progress | Improve section clarity and reduce confusion |
| Template detection | Correctly classifies real CSVs with messy schemas | In progress | Keep broadening schema tolerance |
| Healthcare logic | Supports outcomes, admissions, insurance-risk, claims/fraud shapes | In progress | Keep validating against more public datasets |
| Financial logic | Handles market-export formats like `Close/Last` and currency strings | Done | Add more fixture coverage |
| Reliability/stress testing | Real + synthetic fixtures across all templates | In progress | Expand matrix and keep fixing edge cases |
| Hosted deployment | Netlify + Render stable and connected | Done for MVP | Continue smoke checks after every push |

## 6. Risk Log

| Risk | Why it matters | Mitigation |
|---|---|---|
| Template logic becomes too narrow | Real-world CSVs vary a lot by source | Add alias handling, fallback logic, and fixture coverage |
| UI looks polished but flow breaks on real data | Demo quality can hide product risk | Always test with real CSVs, not just mocks |
| Deployment drift between local and hosted | Hosted bugs can appear even when local works | Run post-deploy smoke tests on Netlify + Render |
| Fixes create regressions in other templates | One detector can affect another | Add targeted regression tests for every bug fix |
| Too many parallel ideas at once | Momentum gets scattered | Keep a “current focus” lane and limit active priorities |

## 7. Current Priority Order

When the project gets noisy, I use this order to decide what gets done first:

1. Any issue that blocks the core flow: upload -> review -> build -> preview
2. Any template detection bug on a real dataset
3. Any regression that breaks live hosted use
4. Any UI confusion that makes the product harder to understand
5. Nice-to-have styling or extra template polish

## 8. What I’m Focused on Right Now

### Immediate focus
- Broaden template detection so Loom works on more real-world CSVs
- Keep adding regression tests from actual user datasets
- Reduce friction in the review and builder stages

### Definition of “on track”
I consider the project on track if:
- the live app works end-to-end for the main flow
- each implemented template has multiple test datasets
- every production bug turns into a permanent regression test
- deployment stays simple enough that updates can be pushed confidently

## 9. Personal Organization Method

My personal method is simple:

- I keep **one current priority** for product work
- I separate **feature work** from **reliability work**
- I convert every bug into a **testable artifact**
- I prefer a **visible tracker + checklist + next-step column** over a large planning document

That keeps the project moving without losing detail, and it makes it easy to explain status to a reviewer, teammate, or stakeholder at any point.

## 10. One-Line Summary

My organization style is: **keep the product moving forward, keep the risks visible, and turn every issue into a reusable system improvement.**

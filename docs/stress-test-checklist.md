# Loom Stress Test Checklist

Use this checklist before major deploys and after any workflow, template, or dashboard-builder changes.

## Release Gate

### Blocker
- Upload fails for a valid CSV
- Blank screen, crash, or uncaught server error
- Template confirmation cannot continue or override safely
- Insight review cannot approve/reject or apply instructions
- Dashboard builder cannot open or preview
- Static HTML export fails
- No path back or start over from review, builder, or preview
- Frontend points at the wrong API target in production

### Major
- Wrong specialized template chosen and override does not recover
- Approved insights do not match builder output
- Section controls are misleading or non-functional
- Hosted cold start is so slow that the app appears broken

### Minor
- Copy issues
- Visual polish issues
- Non-critical chart oddities
- Slow but successful first load on Render

## Local Smoke

1. Run `make dev`
2. Run `make health`
3. Run `make test`
4. Run `make test-templates`
4. Run `make test-ui`

## Hosted Smoke

1. Run `make smoke-prod`
2. Open the live app at [https://loom-analytics.netlify.app](https://loom-analytics.netlify.app)
3. Confirm the landing page renders with Loom branding
4. Upload one canonical fixture for each implemented template:
   - financial
   - e-commerce
   - healthcare
   - marketing
   - HR/workforce
   - survey/sentiment
   - web analytics
5. Build one dashboard for each template and export static HTML

## Template Matrix

Use the checked-in fixture matrix instead of hunting for ad hoc CSVs:

- Manifest: `tests/fixtures/template_manifest.json`
- Canonical stress fixtures:
  - `tests/fixtures/stress/financial/`
  - `tests/fixtures/stress/ecommerce/`
  - `tests/fixtures/stress/healthcare/`
  - `tests/fixtures/stress/hr/`
  - `tests/fixtures/stress/marketing/`
  - `tests/fixtures/stress/survey/`
  - `tests/fixtures/stress/web_analytics/`
  - `tests/fixtures/stress/generic/`

For each implemented template, verify at minimum:
- one `happy_path.csv`
- one `noisy_valid.csv`
- one `ambiguous_schema.csv`
- one `partial_invalid.csv`

Where Loom supports multiple schema families, also use the checked-in schema variants:
- healthcare: `admissions_canon.csv`, `insurance_risk_canon.csv`
- marketing: `lead_generation_canon.csv`, `closed_deals_canon.csv`

## Workflow Checks

### Landing and upload
- Valid CSV uploads begin analysis
- Non-CSV upload shows a readable error
- Very large synthetic CSV still analyzes without crashing

### Template confirmation
- Detected template is visible
- Override to another valid choice works
- Override to generic remains safe and recoverable

### Insight review
- Approve all / reject all both work
- Individual insight toggles work
- Prompt refresh updates focus tags and recommendations
- Back to template works
- Start over returns to landing

### Builder
- Title and subtitle edits persist into preview
- KPI slider changes preview output
- Section on/off controls work
- Up/down ordering is understandable and stable
- Start over returns to landing

### Preview and export
- Preview opens without blank states
- Back to builder works
- Start over works
- Downloaded HTML opens successfully in a browser

## Mobile Checks

- Landing page remains readable and actionable on mobile width
- Review controls remain usable on mobile width
- Preview header remains usable on mobile width

## Dataset Edge Cases

- Empty file
- Header-only CSV
- Duplicate headers
- Mixed numeric/string columns
- Mixed delimiter and alias-heavy files
- UTF-8 BOM
- Long text cells
- Sparse columns
- Partial template schemas
- Ambiguous schemas that should remain generic

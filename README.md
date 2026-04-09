# Loom

Loom is a CSV-to-dashboard product with three layers:

- a core Python pipeline for ingestion, validation, cleaning, and analysis
- a Streamlit prototype that proved the workflow
- a proper React + Vite frontend backed by a lightweight FastAPI API

The product flow is now geared toward business users: upload a CSV, detect the right specialized template, review non-obvious insights, approve the story, and build a shareable dashboard.

## What’s Included

- Modular pipeline stages under `src/pipeline`
- Shared `PipelineContext` contract passed through every stage
- Streamlit prototype for quick iteration
- React/Vite frontend for the proper product UI
- FastAPI API for file upload, template detection, insight review, and dashboard generation
- Specialized business templates for financial time-series, e-commerce, healthcare, and HR/workforce data
- CLI artifacts when persistence is enabled:
- `report.json`
- `summary.md`
- `summary.html`
  - `charts/*.png`
- YAML-driven cleaning configuration
- Fixture-backed tests for ingestion, validation, cleaning, analysis, and end-to-end execution

## Stack

- Python 3.9+
- pandas, numpy, scipy
- matplotlib, seaborn, missingno
- PyYAML, jinja2
- pytest-compatible tests
- Streamlit for the prototype UI
- FastAPI for the product API
- React + Vite + TypeScript for the product frontend
- Tailwind CSS + Lucide + Recharts + dnd-kit for the native dashboard UI
- Playwright for browser stress coverage

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

Run the full product app with one command:

```bash
make dev
```

That starts:
- the FastAPI API on `http://127.0.0.1:8000`
- the React app on `http://127.0.0.1:5173`

Run a quick health check in another terminal:

```bash
make health
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

If you want to run the services separately:

```bash
make api
```

and in a second terminal:

```bash
make frontend
```

The React app proxies API requests to the FastAPI backend on port `8000`.

If you want the frontend to call a non-local backend directly, create [frontend/.env.example](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/frontend/.env.example) as `frontend/.env.local` and set `VITE_API_BASE_URL`.

Run the CLI pipeline directly:

```bash
python -m pipeline.run --input tests/fixtures/normal.csv --output output/demo
```

Launch the Streamlit prototype instead of the React app:

```bash
streamlit run streamlit_app.py
```

Run validation only:

```bash
python -m pipeline.run --input tests/fixtures/normal.csv --output output/validation --validate-only
```

Run with a custom config:

```bash
python -m pipeline.run --input tests/fixtures/missing_values.csv --config config.example.yaml --output output/custom
```

## Output Layout

```text
output/demo/
├── charts/
│   └── *.png
├── logs/
│   └── <run_id>.json
├── report.json
├── summary.html
└── summary.md
```

The Streamlit prototype and the new FastAPI-backed product flow both run in-memory and no longer save per-run artifact folders during interactive use.

## Project Layout

```text
src/pipeline/
├── analysis/
├── api/
├── business/
├── cleaning/
├── common/
├── ingestion/
├── insights/
├── ui/
├── validation/
├── visualization/
└── run.py
frontend/
├── src/
└── vite.config.ts
tests/
└── fixtures/
```

## Configuration

`config.example.yaml` shows the supported MVP knobs:

- numeric missing value strategy
- categorical missing value strategy
- date forward-fill behavior
- duplicate handling
- chart count limit
- top-N segmentation depth

## Testing

If `pytest` is installed:

```bash
pytest --cov=src/pipeline --cov-report=term-missing
```

Without `pytest`, the suite still runs with the standard library:

```bash
python -m unittest discover -s tests
```

Browser stress tests:

```bash
cd frontend
npx playwright install chromium
npm run test:e2e
```

Repo-level stress commands:

```bash
make test-ui
make smoke-prod
make test-stress
```

The hosted smoke runner validates:
- Render health
- one analyze/build pass for each implemented template
- the live Netlify bundle points at the Render backend URL

Optional overrides:

```bash
LOOM_FRONTEND_URL=https://your-frontend.example.com LOOM_BACKEND_URL=https://your-backend.example.com make smoke-prod
```

## Troubleshooting

- If `make dev` fails immediately, make sure `.venv` exists and you ran `npm install` inside [frontend](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/frontend).
- If the React app does not start, make sure you ran `npm install` inside [frontend](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/frontend) first.
- If file uploads fail in the API, make sure your active Python environment has the packages from [requirements.txt](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/requirements.txt), including `fastapi`, `uvicorn`, and `python-multipart`.
- If the frontend loads but API calls fail, confirm the FastAPI server is running on `http://127.0.0.1:8000`.
- If you want to change local ports, set `API_PORT`, `API_HOST`, `FRONTEND_PORT`, or `FRONTEND_HOST` before running [dev.sh](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/scripts/dev.sh).
- If the frontend is hosted separately, make sure `VITE_API_BASE_URL` points at the deployed backend and the backend allows that frontend origin through `APP_CORS_ORIGINS`.

## Deployment

Recommended hosting split:

- Netlify for the React frontend
- Render for the FastAPI backend

Recommended order:

1. Deploy the backend on Render and copy its public URL
2. Deploy the frontend on Netlify with `VITE_API_BASE_URL` pointed at that Render URL
3. Update `APP_CORS_ORIGINS` on Render to your final Netlify site URL
4. Redeploy the Render service once after the Netlify URL is known

### Netlify frontend

This repo includes [netlify.toml](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/netlify.toml), so Netlify can build the frontend from the repo root without extra manual build settings.

Set this environment variable in Netlify:

```bash
VITE_API_BASE_URL=https://your-render-service.onrender.com
```

Then connect the repo in Netlify and deploy. Netlify will build from `frontend/` and publish `frontend/dist`.

### Render backend

This repo includes [render.yaml](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/render.yaml) for the FastAPI service.

Set this environment variable in Render:

```bash
APP_CORS_ORIGINS=https://your-netlify-site.netlify.app
```

Optional:

```bash
APP_CORS_ORIGIN_REGEX=https://.*--your-netlify-site.netlify.app
```

Use the regex only if you also want Netlify preview deploys to talk to the backend.

### Fast path

1. In Render, create a new Blueprint or Web Service from [loom-analytics](https://github.com/aagamshah15/loom-analytics)
2. Approve the settings from [render.yaml](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/render.yaml)
3. Set `APP_CORS_ORIGINS` temporarily to your expected Netlify site URL, or update it right after Netlify gives you the final URL
4. Wait for the backend health check at `/api/health` to pass
5. In Netlify, import the same repo
6. Keep the repo-root [netlify.toml](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/netlify.toml) settings
7. Set `VITE_API_BASE_URL=https://your-render-service.onrender.com`
8. Deploy the frontend
9. If Netlify gives you a different final site URL than expected, update `APP_CORS_ORIGINS` in Render and redeploy once

### Ongoing updates

- push frontend changes to GitHub -> Netlify rebuilds
- push backend changes to GitHub -> Render rebuilds
- if you change the backend URL or frontend domain, update `VITE_API_BASE_URL` and `APP_CORS_ORIGINS`

## Product UI Layers

The proper product frontend lives under [frontend](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/frontend) and talks to the FastAPI app in [app.py](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/src/pipeline/api/app.py).

The Streamlit prototype lives in [app.py](/Users/aagam/Codex%20Challenge/CSV%20Analytics%20Pipeline/src/pipeline/ui/app.py) and remains useful for rapid iteration or fallback demos.

Current React workflow:

- upload a CSV directly in the browser
- run the same core pipeline as the CLI in memory
- confirm or override the detected specialized template
- review deterministic hidden insights one by one and approve/reject them
- accept additional analysis instructions through a prompt box
- build a native React dashboard from the approved insight set using the matched business template
- reorder dashboard sections with simple up/down controls
- switch into preview mode before export
- download the resulting dashboard as a static `.html` file
- review the run summary without exposing pipeline diagnostics

## Current Scope

- CLI-first MVP is implemented and remains the source of truth for data processing
- Streamlit remains the prototype shell
- React/Vite + FastAPI is now the proper frontend direction
- The business layer currently includes financial and e-commerce specialized templates
- Rule-based deterministic insights are implemented now
- LLM narration and predictive models remain post-MVP

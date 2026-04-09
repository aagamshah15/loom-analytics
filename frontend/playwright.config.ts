import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: "list",
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:4173",
    trace: "on-first-retry",
  },
  webServer: process.env.PLAYWRIGHT_BASE_URL
    ? undefined
    : [
        {
          command:
            'bash -lc "cd .. && source .venv/bin/activate && uvicorn pipeline.api.app:app --host 127.0.0.1 --port 8001"',
          url: "http://127.0.0.1:8001/api/health",
          reuseExistingServer: !process.env.CI,
          timeout: 120 * 1000,
        },
        {
          command: 'bash -lc "VITE_PROXY_API_TARGET=http://127.0.0.1:8001 npm run dev -- --host 127.0.0.1 --port 4173"',
          url: "http://127.0.0.1:4173",
          reuseExistingServer: !process.env.CI,
          timeout: 120 * 1000,
        },
      ],
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});

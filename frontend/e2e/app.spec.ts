import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const CURRENT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ECOMMERCE_FIXTURE = path.resolve(CURRENT_DIR, "../../tests/fixtures/stress/ecommerce/happy_path.csv");

test("core Loom journey reaches preview and exports HTML", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByTestId("landing-screen")).toBeVisible();
  await expect(page.getByText("From raw threads to")).toBeVisible();
  await page.getByTestId("landing-nav-templates").click();
  await expect(page.getByTestId("templates-section")).toBeInViewport();
  await expect(page.getByText("Specialized stories, matched to the right data shape")).toBeVisible();
  await page.getByTestId("landing-nav-docs").click();
  await expect(page.getByTestId("docs-section")).toBeInViewport();
  await expect(page.getByText("What Loom expects, and what it does next")).toBeVisible();
  await page.getByTestId("landing-nav-analyze").click();
  await expect(page.getByTestId("analyze-section")).toBeInViewport();

  await page.getByTestId("csv-file-input").setInputFiles(ECOMMERCE_FIXTURE);
  await page.getByTestId("analyze-csv-button").click();

  await expect(page.getByTestId("template-screen")).toBeVisible();
  await expect(page.getByText("Confirm the business template")).toBeVisible();
  await page.getByTestId("continue-to-review-button").click();

  await expect(page.getByTestId("review-screen")).toBeVisible();
  await expect(page.getByTestId("insight-card-discount_paradox")).toBeVisible();

  await page.getByTestId("reject-all-button").click();
  await expect(page.getByTestId("build-dashboard-button")).toBeDisabled();
  await page.getByTestId("approve-all-button").click();
  await expect(page.getByTestId("build-dashboard-button")).toBeEnabled();

  await page.getByTestId("review-prompt-input").fill("focus on discounts and returns");
  await page.getByTestId("apply-instructions-button").click();
  await expect(page.getByText("discounts")).toBeVisible();
  await expect(page.getByText("returns")).toBeVisible();

  await page.getByTestId("build-dashboard-button").click();
  await expect(page.getByTestId("builder-screen")).toBeVisible();
  await expect(page.getByText("Dashboard settings")).toBeVisible();
  await expect(page.getByText("Charts: revenue")).toBeVisible();

  await page.getByTestId("toggle-notes-button").click();
  await page.getByTestId("open-preview-button").click();

  await expect(page.getByTestId("preview-screen")).toBeVisible();
  await expect(page.getByText("Preview")).toBeVisible();
  await expect(page.getByTestId("download-html-button")).toBeVisible();

  const downloadPromise = page.waitForEvent("download");
  await page.getByTestId("download-html-button").click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toMatch(/\.html$/);

  await page.getByRole("button", { name: "Start over" }).click();
  await expect(page.getByTestId("landing-screen")).toBeVisible();
});

test("landing nav surfaces template catalog and docs content", async ({ page }) => {
  await page.goto("/");

  await page.getByTestId("landing-nav-templates").click();
  await expect(page.getByTestId("templates-section")).toBeInViewport();
  await expect(page.getByText("Financial Time Series")).toBeVisible();
  await expect(page.getByText("HR / Workforce")).toBeVisible();
  await expect(page.getByText("Coming soon")).toHaveCount(3);

  await page.getByTestId("landing-nav-docs").click();
  await expect(page.getByTestId("docs-section")).toBeInViewport();
  await expect(page.getByText("Why didn't my file match a specialized template?")).toBeVisible();
  await expect(page.getByRole("link", { name: "View full README" })).toBeVisible();
});

test("template override to generic still reaches review safely", async ({ page }) => {
  await page.goto("/");

  await page.getByTestId("csv-file-input").setInputFiles(ECOMMERCE_FIXTURE);
  await page.getByTestId("analyze-csv-button").click();
  await expect(page.getByTestId("template-screen")).toBeVisible();

  await page.getByTestId("template-option-generic").click();
  await page.getByTestId("apply-template-override-button").click();

  await expect(page.getByTestId("review-screen")).toBeVisible();
  await expect(page.getByTestId("build-dashboard-button")).toBeDisabled();
  await page.getByTestId("back-to-template-button").click();
  await expect(page.getByTestId("template-screen")).toBeVisible();
});

test("invalid upload shows readable 400 error", async ({ page }) => {
  await page.goto("/");

  await page.getByTestId("csv-file-input").setInputFiles({
    name: "notes.txt",
    mimeType: "text/plain",
    buffer: new TextEncoder().encode("not a csv"),
  });
  await page.getByTestId("analyze-csv-button").click();

  await expect(page.getByTestId("app-error-banner")).toContainText("Please upload a CSV file.");
  await expect(page.getByTestId("landing-screen")).toBeVisible();
});

test("server error shows non-blocking banner", async ({ page }) => {
  await page.route("**/api/analyze", async (route) => {
    await route.fulfill({
      status: 500,
      contentType: "text/plain",
      body: "Simulated server error",
    });
  });

  await page.goto("/");
  await page.getByTestId("csv-file-input").setInputFiles(ECOMMERCE_FIXTURE);
  await page.getByTestId("analyze-csv-button").click();

  await expect(page.getByTestId("app-error-banner")).toContainText("Simulated server error");
  await expect(page.getByTestId("landing-screen")).toBeVisible();
});

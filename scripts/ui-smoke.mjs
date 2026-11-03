import { createRequire } from "node:module";
const require = createRequire(
  new URL("../frontend/package.json", import.meta.url),
);
const { chromium } = require("playwright");
import fs from "node:fs";
const browser = await chromium.launch({
  executablePath: process.env.BROWSER_PATH || undefined,
  headless: true,
  args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
});
const page = await browser.newPage({
  viewport: { width: 1440, height: 1100 },
  deviceScaleFactor: 1,
});
const errors = [];
page.on("pageerror", (e) => errors.push(e.message));
await page.goto(process.env.UI_URL || "http://localhost:8088");
await page
  .getByRole("heading", { name: "Vue d’ensemble", exact: true })
  .waitFor();
await page.waitForTimeout(5000);
console.log("title", await page.title(), "errors", errors);
const out = process.env.UI_REPORT_DIR || "/tmp/temperature-ui";
fs.mkdirSync(out, { recursive: true });
await page.screenshot({ path: out + "/tableau-de-bord.png", fullPage: true });
await page.getByRole("button", { name: "Capteurs", exact: true }).click();
await page.getByRole("heading", { name: "Parc de capteurs" }).waitFor();
await page.getByRole("button", { name: "Ajouter un capteur" }).click();
const id = "UI-" + Date.now();
await page.getByRole("textbox", { name: "Identifiant" }).fill(id);
await page.getByRole("textbox", { name: "Libellé" }).fill("Test navigateur");
await page
  .getByRole("textbox", { name: "Salle", exact: true })
  .fill("Salle navigateur");
await page.getByRole("button", { name: "Enregistrer", exact: true }).click();
await page.getByRole("dialog").waitFor({ state: "hidden" });
await page.getByText(id, { exact: true }).waitFor();
await page
  .getByRole("button", { name: "Envoyer une mesure", exact: true })
  .click();
await page.locator('select[name="capteurId"]').selectOption(id);
await page.locator('input[name="temperature"]').fill("46");
await page
  .getByRole("button", { name: "Envoyer la mesure", exact: true })
  .click();
await page.getByRole("dialog").waitFor({ state: "hidden" });
await page.getByText("Traitée", { exact: true }).waitFor({ timeout: 60000 });
const row = page
  .locator("tr")
  .filter({ has: page.getByText(id, { exact: true }) });
await row.getByRole("button", { name: "Modifier" }).click();
await page.locator('input[name="libelle"]').fill("Modifié par le navigateur");
await page.getByRole("button", { name: "Enregistrer", exact: true }).click();
await page.getByRole("dialog").waitFor({ state: "hidden" });
await page.getByText("Modifié par le navigateur", { exact: true }).waitFor();
await page.getByRole("button", { name: /Alertes/ }).click();
const alert = page
  .locator("tr")
  .filter({ has: page.getByText(id, { exact: true }) });
await alert.getByRole("button", { name: "Acquitter" }).click();
await page.getByText("Alerte acquittée.", { exact: true }).waitFor();
await page.getByRole("button", { name: "Historique", exact: true }).click();
await page.getByLabel("Capteur", { exact: true }).selectOption(id);
await page.getByRole("button", { name: "Exporter CSV" }).waitFor();
const downloadPromise = page.waitForEvent("download");
await page.getByRole("button", { name: "Exporter CSV" }).click();
const download = await downloadPromise;
console.log("download", download.suggestedFilename());
await page.screenshot({ path: out + "/historique.png", fullPage: true });
await page.setViewportSize({ width: 390, height: 844 });
await page.getByRole("button", { name: "Menu", exact: true }).click();
await page.getByRole("button", { name: "Vue d’ensemble", exact: true }).click();
await page.waitForTimeout(1500);
await page.screenshot({ path: out + "/mobile.png", fullPage: true });
const overflow = await page.evaluate(
  () => document.documentElement.scrollWidth > innerWidth,
);
if (overflow) throw Error("Débordement mobile");
if (errors.length) throw Error(errors.join("\n"));
console.log(
  "PASS navigation, création, mesure, suivi, édition, acquittement, export CSV, mobile",
);
await browser.close();

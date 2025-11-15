/**
 * Portal Builder
 * 
 * Builds the root portal index.html that lists all microsites.
 * This is separate from the microsite builder (build.js) to prevent conflicts.
 * 
 * The portal index.html is deployed to the ROOT of the GCS bucket,
 * while individual microsites are in subdirectories (e.g., /mayhem-maker/)
 */

const fs = require("fs/promises");
const path = require("path");

const ROOT = __dirname;
const PORTAL_SOURCE = path.join(ROOT, "portal");
const PORTAL_DIST = path.join(ROOT, "portal-dist");

async function copyDir(src, dest) {
  try {
    await fs.rm(dest, { recursive: true, force: true });
    await fs.mkdir(dest, { recursive: true });
    await fs.cp(src, dest, { recursive: true });
  } catch (err) {
    console.error(`[portal-build] Failed to copy ${src} -> ${dest}`, err);
    throw err;
  }
}

async function main() {
  console.log("[portal-build] Building portal...");

  // Copy portal files to portal-dist
  await copyDir(PORTAL_SOURCE, PORTAL_DIST);

  // Copy shared assets if needed
  const assetsSource = path.join(ROOT, "images");
  const assetsDest = path.join(PORTAL_DIST, "images");

  try {
    await fs.access(assetsSource);
    await fs.mkdir(assetsDest, { recursive: true });
    await fs.cp(assetsSource, assetsDest, { recursive: true });
    console.log("[portal-build] Copied shared images");
  } catch (err) {
    console.warn("[portal-build] No images directory to copy");
  }

  console.log(`[portal-build] Portal built successfully in ${PORTAL_DIST}`);
  console.log("[portal-build] Ready for deployment to GCS root");
}

main().catch((err) => {
  console.error("[portal-build] Build failed", err);
  process.exit(1);
});

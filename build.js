const fs = require("fs/promises");
const path = require("path");
const nunjucks = require("nunjucks");

const ROOT = __dirname;
const DIST_ROOT = path.join(ROOT, "dist");
const TEMPLATE_ROOT = path.join(ROOT, "_templates");
const IMAGES_ROOT = path.join(ROOT, "images");
const ASSETS_ROOT = path.join(ROOT, "assets");
const DATA_PATH = path.join(ROOT, "videos.json");
const CONFIG_PATH = path.join(ROOT, "site.config.json");
const PKG = require("./package.json");

const DEFAULT_BASE_URL = (process.env.SITE_BASE_URL || process.env.PUBLIC_SITES_BASE_URL || "https://sites.local").replace(/\/$/, "");
const APP_VERSION = PKG.version || "0.1.0";

function sanitizeSlug(slug) {
  return (slug || "site")
    .toString()
    .toLowerCase()
    .replace(/[^a-z0-9\-_/]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/\/+/g, "/") || "site";
}

function slugifyTitle(title, fallback) {
  const safe = (title || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  const suffix = (fallback || "video").slice(0, 6).toLowerCase();
  return `${safe || "video"}-${suffix}`;
}

function shortDescription(desc) {
  const trimmed = (desc || "").trim();
  if (!trimmed) return "";
  const collapsed = trimmed.replace(/\s+/g, " ");
  return collapsed.length > 180 ? `${collapsed.slice(0, 177)}...` : collapsed;
}

function linkifyDescription(desc) {
  const escaped = (desc || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  const withLinks = escaped.replace(/(https?:\/\/[^\s]+)/gi, '<a class="link" href="$1" target="_blank" rel="noopener">$1</a>');
  return withLinks.replace(/\r?\n/g, "<br>");
}

function durationText(seconds) {
  if (seconds == null || Number.isNaN(seconds)) return "";
  const s = Math.max(0, Math.floor(Number(seconds)));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) {
    return `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}:${sec.toString().padStart(2, "0")}`;
  }
  return `${m.toString().padStart(2, "0")}:${sec.toString().padStart(2, "0")}`;
}

function buildItemListSchema(videos, siteUrl) {
  return {
    "@context": "https://schema.org",
    "@type": "ItemList",
    itemListElement: videos.slice(0, 20).map((video, index) => ({
      "@type": "ListItem",
      position: index + 1,
      url: `${siteUrl}/videos/${video.slug}.html`,
      name: video.title,
    })),
  };
}

function buildWebPageSchema(siteName, siteUrl) {
  return {
    "@context": "https://schema.org",
    "@type": "WebPage",
    name: siteName,
    url: siteUrl,
    inLanguage: "en",
  };
}

function buildVideoSchema(video, siteMeta, channel) {
  return {
    "@context": "https://schema.org",
    "@type": "VideoObject",
    name: video.title,
    description: video.short_desc,
    thumbnailUrl: `https://i.ytimg.com/vi/${video.video_id}/hqdefault.jpg`,
    uploadDate: video.published_at || undefined,
    publisher: {
      "@type": "Organization",
      name: channel.title || siteMeta.siteName,
    },
    contentUrl: `https://www.youtube.com/watch?v=${video.video_id}`,
    embedUrl: `https://www.youtube.com/embed/${video.video_id}`,
  };
}

function buildFaqSchema() {
  return {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: [],
  };
}

async function readJson(file, fallback) {
  try {
    const raw = await fs.readFile(file, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    if (err.code === "ENOENT") return fallback;
    throw err;
  }
}

async function copyDir(src, dest) {
  try {
    await fs.rm(dest, { recursive: true, force: true });
    await fs.cp(src, dest, { recursive: true });
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.warn(`[mm-site] failed to copy ${src} -> ${dest}`, err);
    }
  }
}

function ensureArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

async function renderDisabledSite(meta, buildStamp, siteDir) {
  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${meta.siteName}</title>
  <style>
    body{margin:0;min-height:100vh;display:grid;place-items:center;background:#05060c;color:#e2e8f0;font-family:system-ui}
    .card{max-width:520px;padding:32px;border-radius:12px;background:#111827;border:1px solid rgba(148,163,184,0.2);text-align:center}
    h1{margin:0 0 12px;font-size:1.75rem}
    p{margin:0;color:#94a3b8;line-height:1.5}
  </style>
</head>
<body>
  <div class="card">
    <h1>${meta.siteName}</h1>
    <p>This site is currently hidden. The owner disabled public access on ${new Date(buildStamp).toLocaleString()}.</p>
  </div>
</body>
</html>`;
  await fs.writeFile(path.join(siteDir, "index.html"), html, "utf8");
  const metaJson = {
    siteEnabled: false,
    siteName: meta.siteName,
    generatedAt: buildStamp,
  };
  await fs.writeFile(path.join(siteDir, "meta.json"), JSON.stringify(metaJson, null, 2), "utf8");
}

async function main() {
  const config = await readJson(CONFIG_PATH, {});
  const data = await readJson(DATA_PATH, []);
  if (!Array.isArray(data) || data.length === 0) {
    throw new Error("videos.json is empty � cannot build microsite");
  }

  const buildStamp = new Date().toISOString();
  const slug = sanitizeSlug(config.slug || config.siteSlug || data[0]?.slug || data[0]?.channel_title || "microsite");
  const siteUrl = (config.siteUrl || `${DEFAULT_BASE_URL}/${slug}`).replace(/\/$/, "");
  const siteName = config.siteName || data[0]?.channel_title || "Your channel";
  const siteDescription = config.siteDescription || "Explore videos and updates.";
  const siteLogoUrl = config.siteLogoUrl || "/images/JasonMartocciLogo.webp";
  const siteEnabled = config.siteEnabled !== false;

  const channel = {
    title: config.channelTitle || data[0]?.channel_title || siteName,
    handle: config.channelHandle || `@${sanitizeSlug((data[0]?.channel_title || siteName).replace(/[^a-z0-9]+/gi, ""))}`,
    subscriberCount: toNumber(config.subscriberCount ?? data[0]?.subs, 0),
  };

  await fs.rm(DIST_ROOT, { recursive: true, force: true });
  await fs.mkdir(DIST_ROOT, { recursive: true });

  await copyDir(IMAGES_ROOT, path.join(DIST_ROOT, "images"));
  await copyDir(ASSETS_ROOT, path.join(DIST_ROOT, "assets"));

  const staticFiles = ["robots.txt", "sitemap.xml", "indexnowkey.txt", "indexnow_key.txt", "googlef496381b95da9f1d.html", "CNAME"];
  for (const file of staticFiles) {
    const src = path.join(ROOT, file);
    try {
      await fs.copyFile(src, path.join(DIST_ROOT, file));
    } catch (err) {
      if (err.code !== "ENOENT") {
        console.warn(`[mm-site] unable to copy ${file}`, err);
      }
    }
  }

  const meta = { siteName, siteDescription, siteLogoUrl, siteUrl, siteEnabled };

  // Social media links with fallbacks
  const socialX = config.socialX || "https://x.com/MartocciMayhem";
  const socialTikTok = config.socialTikTok || "https://www.tiktok.com/@MartocciMayhem";
  const socialYouTube = config.socialYouTube || `https://www.youtube.com/@${channel.handle.replace('@', '')}`;
  const socialInstagram = config.socialInstagram || "https://www.instagram.com/MartocciMayhem";
  const socialFacebook = config.socialFacebook || "https://www.facebook.com/MartocciMayhem";
  const socialLinkedIn = config.socialLinkedIn || "https://www.linkedin.com/company/MartocciMayhem";

  if (!siteEnabled) {
    await renderDisabledSite(meta, buildStamp, DIST_ROOT);
    console.log(`[mm-site] site disabled via configuration, wrote placeholder to ${DIST_ROOT}`);
    return;
  }

  const env = new nunjucks.Environment(new nunjucks.FileSystemLoader(TEMPLATE_ROOT), {
    autoescape: false,
    trimBlocks: false,
    lstripBlocks: false,
  });
  env.addFilter("tojson", (value) => JSON.stringify(value));

  const videos = data.map((video) => {
    const slugValue = sanitizeSlug(video.slug || slugifyTitle(video.title || "video", video.video_id || "vid"));
    const desc = String(video.desc || "");
    const published = video.upload_date || video.last_edited_date || video.creation_date || null;
    return {
      video_id: video.video_id,
      slug: slugValue,
      title: video.title || "Untitled video",
      desc,
      short_desc: shortDescription(desc),
      formatted_desc: linkifyDescription(desc),
      tags: ensureArray(video.tags).map(String),
      category: video.category || "People & Blogs",
      published_at: published,
      last_edited_date: video.last_edited_date || published || buildStamp,
      view_count: toNumber(video.view_count),
      like_count: toNumber(video.like_count),
      comment_count: toNumber(video.comment_count),
      duration_text: durationText(video.duration_seconds),
    };
  });

  videos.sort((a, b) => String(b.last_edited_date || "").localeCompare(String(a.last_edited_date || "")));

  const itemListSchema = buildItemListSchema(videos, siteUrl);
  const webPageSchema = buildWebPageSchema(siteName, siteUrl);

  const indexHtml = env.render("index_template.html", {
    app_version: APP_VERSION,
    build_stamp: buildStamp,
    site_url: siteUrl,
    site_name: siteName,
    site_description: siteDescription,
    videos,
    item_list_schema: itemListSchema,
    web_page_schema: webPageSchema,
    socialX,
    socialTikTok,
    socialYouTube,
    socialInstagram,
    socialFacebook,
    socialLinkedIn,
  });

  await fs.writeFile(path.join(DIST_ROOT, "index.html"), indexHtml, "utf8");

  const videosDir = path.join(DIST_ROOT, "videos");
  await fs.mkdir(videosDir, { recursive: true });

  for (const video of videos) {
    const related = videos.filter((v) => v.slug !== video.slug).slice(0, 6).map((v) => ({
      slug: v.slug,
      title: v.title,
      desc: v.short_desc,
      video_id: v.video_id,
      view_count: v.view_count,
    }));

    const html = env.render("video_template.html", {
      app_version: APP_VERSION,
      build_stamp: buildStamp,
      site_url: siteUrl,
      site_logo_url: siteLogoUrl,
      title: video.title,
      short_desc: video.short_desc,
      desc: video.desc,
      formatted_desc: video.formatted_desc,
      slug: video.slug,
      video_id: video.video_id,
      published_date: video.published_at,
      view_count: video.view_count.toLocaleString("en-US"),
      like_count: video.like_count.toLocaleString("en-US"),
      comment_count: video.comment_count.toLocaleString("en-US"),
      duration_text: video.duration_text,
      category: video.category,
      tags: video.tags,
      channel_title: channel.title,
      channel_handle: channel.handle,
      subscriber_count: channel.subscriberCount.toLocaleString("en-US"),
      subs_known: channel.subscriberCount > 0,
      video_schema: buildVideoSchema(video, meta, channel),
      faq_schema: buildFaqSchema(),
      related,
      socialX,
      socialTikTok,
      socialYouTube,
      socialInstagram,
      socialFacebook,
      socialLinkedIn,
    });

    await fs.writeFile(path.join(videosDir, `${video.slug}.html`), html, "utf8");
  }

  const metaJson = {
    siteEnabled: true,
    siteName,
    siteUrl,
    siteDescription,
    siteLogoUrl,
    generatedAt: buildStamp,
    videoCount: videos.length,
    channel,
  };
  await fs.writeFile(path.join(DIST_ROOT, "meta.json"), JSON.stringify(metaJson, null, 2), "utf8");
  await fs.writeFile(path.join(DIST_ROOT, "videos.json"), JSON.stringify(videos, null, 2), "utf8");

  const slugMap = Object.fromEntries(videos.map((video) => [video.video_id, video.slug]));
  await fs.writeFile(path.join(DIST_ROOT, "slugs.json"), JSON.stringify(slugMap, null, 2), "utf8");

  console.log(`[mm-site] build completed for slug "${slug}" - files in ${DIST_ROOT}`);
}

main().catch((err) => {
  console.error("[mm-site] build failed", err);
  process.exit(1);
});


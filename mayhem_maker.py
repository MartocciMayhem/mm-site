#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Mayhem Maker (GUI + CLI)
- Zero-API site renderer from cached videos.json
- Smart refresh / fetch via YouTube Data API v3 (optional)
- Dry-run with diffs into _preview/
- Git deploy + IndexNow + Google sitemap ping
- Insights & logs
Tested on Python 3.10–3.12
pip install dearpygui jinja2 gitpython requests python-dotenv
(Optional) pip install google-api-python-client google-auth-oauthlib plyer
"""

from __future__ import annotations
import os, re, sys, json, uuid, difflib, logging, argparse, threading
import datetime as dt
from functools import lru_cache
from urllib.parse import urlsplit, parse_qs
import pickle, mimetypes, time

import html as _html

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # Python 3.9+
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception  # fallback

try:
    PT_TZ = ZoneInfo("America/Los_Angeles") if ZoneInfo else None
except ZoneInfoNotFoundError:
    PT_TZ = None

def _today_pacific() -> dt.date:
    if PT_TZ:
        return dt.datetime.now(PT_TZ).date()
    # Fallback if zoneinfo/tzdata isn't available
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=8)).date()

# --- Third-party (soft-fail so GUI can explain what’s missing) ---
_missing: list[tuple[str,str]] = []

# --- YouTube OAuth editor (soft-fail) ---
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    from google_auth_oauthlib.flow import InstalledAppFlow
except Exception as e:
    _missing.append(("google-api-python-client / google-auth-oauthlib", str(e)))

try:
    import dearpygui.dearpygui as dpg
except Exception as e:
    dpg = None  # ensure symbol exists so guards like "if not dpg" work
    _missing.append(("dearpygui", str(e)))
try:
    from git import Repo
except Exception as e:
    _missing.append(("gitpython", str(e)))
try:
    import jinja2
except Exception as e:
    _missing.append(("jinja2", str(e)))
try:
    import requests
except Exception as e:
    _missing.append(("requests", str(e)))
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
try:
    from plyer import notification as plyer_notify
except Exception:
    plyer_notify = None

class QuotaExceeded(RuntimeError):
    pass

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("MM")

# ---------- Constants & Defaults ----------
APP_NAME = "Mayhem Maker"
SETTINGS_FILE = "mm_settings.json"
QUOTA_FILE    = "quota_tracker.json"
VIDEOS_JSON   = "videos.json"
TEMPLATE_DIR_NAME = "_templates"
PREVIEW_DIR_NAME  = "_preview"      # dry-run output
VIDEOS_DIR_NAME   = "videos"
APP_VERSION = "0.7.0"
BUILD_STAMP = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d.%H%M%S")

DAILY_QUOTA_LIMIT = 10000
API_COSTS = {
    "videos.list": 1,
    "search.list": 100,
    "commentThreads.list": 1,
    "channels.list": 1,
    # editor-only (estimations)
    "videos.update": 50,
    "thumbnails.set": 50,
    "playlistItems.list": 1,
}

CATEGORIES = {
    '1': 'Film & Animation','2': 'Autos & Vehicles','10': 'Music','15': 'Pets & Animals','17': 'Sports',
    '18': 'Short Movies','19': 'Travel & Events','20': 'Gaming','21': 'Videoblogging','22': 'People & Blogs',
    '23': 'Comedy','24': 'Entertainment','25': 'News & Politics','26': 'Howto & Style','27': 'Education',
    '28': 'Science & Technology','29': 'Nonprofits & Activism','30': 'Movies','31': 'Anime/Animation',
    '32': 'Action/Adventure','33': 'Classics','34': 'Comedy','35': 'Documentary','36': 'Drama','37': 'Family',
    '38': 'Foreign','39': 'Horror','40': 'Sci-Fi/Fantasy','41': 'Thriller','42': 'Shorts','43': 'Shows','44': 'Trailers',
}
CATEGORY_BY_NAME = {}
for k, v in CATEGORIES.items():
    CATEGORY_BY_NAME.setdefault(v, k)  # first wins

CATEGORY_CHOICES = ["no_change"] + [
    f"{k} — {v}" for k, v in sorted(CATEGORIES.items(), key=lambda kv: (kv[1], int(kv[0])))
]

def _cat_id_from_selection(sel: str) -> str | None:
    if not sel or sel == "no_change": return None
    if "—" in sel:  # "22 — People & Blogs"
        return sel.split("—", 1)[0].strip()
    # allow choosing by name
    return CATEGORY_BY_NAME.get(sel, sel)

DEFAULTS = {
    "REPO_PATH": r"...",
    "SITE_URL": "https://MartocciMayhem.com",
    "CHANNEL_HANDLE": "@MartocciMayhem",
    "YOUTUBE_API_KEY": "",
    "SMART_REFRESH_DAYS": 7,
    "ENABLE_INDEXNOW": True,
    "ENABLE_GOOGLE_PING": True,
    "DEFAULT_COMMIT_MSG": f"Update site v{APP_VERSION} ({BUILD_STAMP})",
    "THEME": "auto",
    "NOTIFY_ON_COMPLETE": True,
    "OUTPUT_NAMING": "slug"
}

# --- Utilities ---
def _open_path(p: str):
    """Open a folder in the OS file browser."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(p)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            os.system(f'open "{p}"')
        else:
            os.system(f'xdg-open "{p}" >/dev/null 2>&1 || printf "%s\n" "{p}"')
    except Exception as e:
        log.warning("Open path failed for %s: %s", p, e)

# ---- Lightweight HTTP wrapper with quota detection ----
def yt_get(url: str, headers: dict | None = None, timeout: int = 12):
    if "requests" in [m[0] for m in _missing]:
        raise RuntimeError("requests not installed")
    r = requests.get(url, headers=headers or {}, timeout=timeout)

    if r.status_code in (200, 304):
        return r

    if r.status_code == 429:
        raise QuotaExceeded("YouTube API rate limited (429)")

    try:
        j = r.json()
    except Exception:
        j = {}
    err = j.get("error", {})

    if isinstance(err, dict) and err.get("code") == 403 and any(
        k in str(err).lower() for k in ("quotaexceeded", "dailylimitexceeded", "ratelimitexceeded")
    ):
        raise QuotaExceeded("YouTube API quota exceeded")

    raise RuntimeError(f"YouTube API error {r.status_code}: {r.text[:200]}")

# ---- oEmbed title fetch (no API key required) ----
def fetch_title_oembed(video_id: str) -> str:
    if "requests" in [m[0] for m in _missing]:
        return ""
    try:
        url = f"https://www.youtube.com/oembed?format=json&url=https://www.youtube.com/watch?v={video_id}"
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            j = r.json()
            return (j.get("title") or "").strip()
    except Exception:
        pass
    return ""

# ---- Emoji / UI Fonts (cross-platform & safe) ----
def first_existing(paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

def prepare_fonts():
    import inspect

    fonts = {"ui": None, "h1": None, "mono": None}
    if not dpg:
        return fonts

    # If fonts already exist, just return the aliases (prevents "Alias already exists")
    if dpg.does_item_exist("__font_ui") or dpg.does_item_exist("__font_h1") or dpg.does_item_exist("__font_mono"):
        return {
            "ui": "__font_ui" if dpg.does_item_exist("__font_ui") else None,
            "h1": "__font_h1" if dpg.does_item_exist("__font_h1") else None,
            "mono": "__font_mono" if dpg.does_item_exist("__font_mono") else None,
        }

    # Does this DPG build support merge_mode on add_font?
    try:
        _add_font_params = inspect.signature(dpg.add_font).parameters
        _supports_merge_mode = "merge_mode" in _add_font_params
    except Exception:
        _supports_merge_mode = False

    base_candidates = [
        r"C:\Windows\Fonts\segoeui.ttf", r"C:\Windows\Fonts\arial.ttf",
        "/System/Library/Fonts/SFNS.ttf", "/System/Library/Fonts/SFNSDisplay.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
    ]
    mono_candidates = [
        r"C:\Windows\Fonts\consola.ttf",
        "/System/Library/Fonts/Monaco.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    ]
    emoji_candidates = [
        r"C:\Windows\Fonts\seguiemj.ttf",         # Segoe UI Emoji (Windows)
        "/System/Library/Fonts/Apple Color Emoji.ttc",
        "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
    ]

    base = first_existing(base_candidates)
    mono = first_existing(mono_candidates) or base
    emoji = first_existing(emoji_candidates)

    with dpg.font_registry():
        # Main UI font
        ui = dpg.add_font(base, 20, tag="__font_ui") if base else None
        if ui:
            dpg.add_font_range(0x0020, 0x00FF, parent=ui)
            dpg.add_font_range(0x0100, 0x024F, parent=ui)

            # Try to merge emoji
            if emoji:
                if _supports_merge_mode:
                    try:
                        dpg.add_font(emoji, 20, merge_mode=True)
                    except Exception as e:
                        log.warning("Emoji merge failed; continuing without merge: %s", e)
                else:
                    # Old DPG: no merge_mode support — skip merging to avoid crash
                    log.warning("Dear PyGui build does not support merge_mode; skipping emoji merge.")

                # Provide the code points we care about (safe whether merge happened or not)
                try:
                    dpg.add_font_chars(chars=[
                        0x1F525,  # 🔥
                        0x1F4A5,  # 💥
                        0x1F527,  # 🔧
                        0x26A1,   # ⚡
                        0x1F680,  # 🚀
                        0x1F9EA,  # 🧪
                        0x1F4E6,  # 📦
                        0x1F504,  # 🔄
                        0x2699,   # ⚙
                    ], parent=ui)
                except Exception:
                    pass

            fonts["ui"] = ui

        # Header (bigger)
        if base:
            h1 = dpg.add_font(base, 24, tag="__font_h1")
            dpg.add_font_range(0x0020, 0x00FF, parent=h1)
            dpg.add_font_range(0x0100, 0x024F, parent=h1)
            if emoji:
                if _supports_merge_mode:
                    try:
                        dpg.add_font(emoji, 24, merge_mode=True)
                    except Exception as e:
                        log.warning("Emoji merge(h1) failed; continuing without merge: %s", e)
                else:
                    log.warning("Dear PyGui build does not support merge_mode; skipping emoji merge (h1).")
                try:
                    dpg.add_font_chars(chars=[0x1F525, 0x1F4A5, 0x2699], parent=h1)
                except Exception:
                    pass
            fonts["h1"] = h1

        # Monospace
        if mono:
            m = dpg.add_font(mono, 18, tag="__font_mono")
            dpg.add_font_range(0x0020, 0x00FF, parent=m)
            fonts["mono"] = m

    return fonts

def _load_logo_texture(path, tag="__logo_tx"):
    try:
        if os.path.exists(path):
            w, h, c, data = dpg.load_image(path)
            with dpg.texture_registry(show=False):
                if not dpg.does_item_exist(tag):
                    dpg.add_static_texture(w, h, data, tag=tag)
            return tag, w, h
    except Exception:
        pass
    return None, 0, 0

# ---- YouTube Data API helpers (used by fetch_* and refresh) ----
@lru_cache(maxsize=1)
def fetch_channel_details() -> dict:
    """Get channel id/title/subs for the configured handle."""
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set")
    base = "https://www.googleapis.com/youtube/v3/channels"
    url  = f"{base}?part=snippet,statistics&forHandle={CHANNEL_HANDLE.lstrip('@')}&key={YOUTUBE_API_KEY}"
    r = yt_get(url)
    add_quota_usage("channels.list")
    data = r.json()
    items = data.get("items") or []
    if not items:
        raise RuntimeError("Channel not found")
    item = items[0]
    return {
        "id": item["id"],
        "title": item["snippet"]["title"],
        "subs": int(item.get("statistics", {}).get("subscriberCount", 0) or 0)
    }

def fetch_video_details(video_id: str, etag: str | None = None):
    """Return (video_item_json_or_None, new_etag). None when 304 (unchanged)."""
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set")
    base = "https://www.googleapis.com/youtube/v3/videos"
    url  = f"{base}?part=snippet,statistics,status,contentDetails&id={video_id}&key={YOUTUBE_API_KEY}"
    headers = {"If-None-Match": etag} if etag else {}
    r = yt_get(url, headers=headers)
    add_quota_usage("videos.list")
    if r.status_code == 304:
        return None, etag
    items = r.json().get("items", [])
    if not items:
        raise RuntimeError(f"Video not found: {video_id}")
    return items[0], r.headers.get("ETag")

def fetch_list_by_channel(max_results: int = 50, page_token: str | None = None, published_after: str | None = None):
    """List videos on the channel (search.list)."""
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set")
    ch = fetch_channel_details()
    base = "https://www.googleapis.com/youtube/v3/search"
    url  = f"{base}?part=snippet&channelId={ch['id']}&order=date&type=video&maxResults={max_results}"
    if published_after:
        url += f"&publishedAfter={published_after}"
    if page_token:
        url += f"&pageToken={page_token}"
    url += f"&key={YOUTUBE_API_KEY}"
    r = yt_get(url)
    add_quota_usage("search.list")
    return r.json()

# ---- Reslug helpers used by CLI ----
def reslug_cache_from_title(only_if_id_like: bool = True):
    """
    Rebuild slugs from each video's title.
    When only_if_id_like=True, keep curated slugs and only replace 'ID-like' ones.
    """
    videos = load_cache()
    changes = []

    def is_id_like(slug: str | None, vid: str) -> bool:
        s = (slug or "").lower()
        v = (vid or "").lower()
        if s == v:
            return True
        return bool(re.fullmatch(r"[a-z0-9_-]{10,15}", s))

    for v in videos:
        vid = v.get("video_id", "")
        old = (v.get("slug") or "").strip()
        new = slugify(v.get("title") or vid)
        if not new:
            continue
        if only_if_id_like and old and not is_id_like(old, vid):
            continue
        if old != new:
            v["slug"] = new
            changes.append((vid, old, new))

    if changes:
        save_cache(videos)
    return changes

def reslug_cache_from_oembed(only_if_id_like: bool = True):
    """
    Use YouTube's oEmbed endpoint (no API key) to fetch titles and rebuild slugs.
    Only touches 'ID-like' slugs by default.
    """
    videos = load_cache()
    changes = []

    def is_id_like(slug: str | None, vid: str) -> bool:
        s = (slug or "").lower()
        v = (vid or "").lower()
        if s == v or s == f"video-{v}":
            return True
        return bool(re.fullmatch(r"[a-z0-9_-]{10,15}", s))

    def title_is_placeholder(title: str | None, vid: str) -> bool:
        t = (title or "").strip()
        return bool(re.fullmatch(r"(?i)video\s+" + re.escape(vid), t))

    for v in videos:
        vid = v.get("video_id", "")
        old_slug = (v.get("slug") or "").strip()
        old_title = (v.get("title") or "").strip()

        if only_if_id_like and not is_id_like(old_slug, vid):
            continue
        if old_title and not title_is_placeholder(old_title, vid):
            continue

        new_title = fetch_title_oembed(vid)
        if not new_title:
            continue

        v["title"] = new_title
        new_slug = slugify(new_title) or slugify(vid)
        if old_slug != new_slug:
            v["slug"] = new_slug
            changes.append((vid, old_slug, new_slug))

    if changes:
        save_cache(videos)
    return changes

# ---------- Settings ----------
def load_settings() -> dict:
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass
    s = DEFAULTS.copy()
    env_overrides = {k: os.getenv(k) for k in ["REPO_PATH","SITE_URL","CHANNEL_HANDLE","YOUTUBE_API_KEY"]}
    for k,v in env_overrides.items():
        if v: s[k] = v
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                s.update(json.load(f))
        except Exception as e:
            log.warning("Failed to load %s: %s", SETTINGS_FILE, e)
    s["REPO_PATH"] = os.path.expanduser(s["REPO_PATH"])
    return s

def save_settings(s:dict):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception as e:
        log.error("Failed to save %s: %s", SETTINGS_FILE, e)

SET = load_settings()

# ---------- Paths ----------
REPO_PATH   = SET["REPO_PATH"]
SITE_URL    = SET["SITE_URL"].rstrip("/")
CHANNEL_HANDLE = SET["CHANNEL_HANDLE"]
YOUTUBE_API_KEY= SET["YOUTUBE_API_KEY"]
TEMPLATE_DIR= os.path.join(REPO_PATH, TEMPLATE_DIR_NAME)
VIDEOS_DIR  = os.path.join(REPO_PATH, VIDEOS_DIR_NAME)
PREVIEW_DIR = os.path.join(REPO_PATH, PREVIEW_DIR_NAME)
for p in [REPO_PATH, TEMPLATE_DIR, VIDEOS_DIR, PREVIEW_DIR]:
    os.makedirs(p, exist_ok=True)

# ---------- Removed registry (no hacks; persisted) ----------
REMOVED_JSON = "removed.json"

def load_removed() -> set[str]:
    p = os.path.join(REPO_PATH, REMOVED_JSON)
    try:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return set(json.load(f))
    except Exception:
        pass
    return set()

def save_removed(ids:set[str]):
    p = os.path.join(REPO_PATH, REMOVED_JSON)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(sorted(ids), f, indent=2)

# ---------- Quota ----------
estimated_units = 0
last_quota_date_pt = _today_pacific()
per_method_today: dict[str, int] = {}

def _quota_path():
    return os.path.join(REPO_PATH, QUOTA_FILE)

def load_quota():
    global estimated_units, last_quota_date_pt, per_method_today
    p = _quota_path()
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            last = d.get("date_pt")
            estimated_units = int(d.get("units", 0))
            per_method_today = dict(d.get("per_method", {}))
            if last:
                last_quota_date_pt = dt.date.fromisoformat(last)
        except Exception as e:
            log.warning("Failed loading quota: %s", e)

    # reset only when the *Pacific* day changes
    today_pt = _today_pacific()
    if last_quota_date_pt < today_pt:
        estimated_units = 0
        per_method_today = {}
        last_quota_date_pt = today_pt
        save_quota()

def save_quota():
    p = _quota_path()
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump({
                "date_pt": last_quota_date_pt.isoformat(),
                "units": int(estimated_units),
                "per_method": per_method_today,
            }, f, indent=2)
    except Exception as e:
        log.error("Failed saving quota: %s", e)

def add_quota_usage(method: str, mult: int = 1) -> int:
    """Count cost and persist, rolling the day at PT midnight."""
    global estimated_units, last_quota_date_pt, per_method_today
    today_pt = _today_pacific()
    if last_quota_date_pt < today_pt:
        estimated_units = 0
        per_method_today = {}
        last_quota_date_pt = today_pt

    cost = API_COSTS.get(method, 0) * int(mult or 1)
    estimated_units += cost
    per_method_today[method] = per_method_today.get(method, 0) + cost
    save_quota()
    try: _reload_quota_text()
    except Exception: pass
    return cost

def remaining_quota() -> int:
    return max(0, DAILY_QUOTA_LIMIT - int(estimated_units))

def mark_quota_exhausted():
    """Call this if you catch a 403/429 quota error—remaining is 0 for the rest of the PT day."""
    global estimated_units
    estimated_units = DAILY_QUOTA_LIMIT
    save_quota()
    try: _reload_quota_text()
    except Exception: pass

# ---------- Cache ----------
def load_cache() -> list[dict]:
    path = os.path.join(REPO_PATH, VIDEOS_JSON)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error("Failed to load %s: %s", VIDEOS_JSON, e)
    return []

def save_cache(videos:list[dict]):
    path = os.path.join(REPO_PATH, VIDEOS_JSON)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(videos, f, indent=2)
    except Exception as e:
        log.error("Failed to save %s: %s", VIDEOS_JSON, e)

# ---------- IndexNow key ----------
INDEXNOW_KEY_FILE = os.path.join(REPO_PATH, "indexnow_key.txt")
if not os.path.exists(INDEXNOW_KEY_FILE):
    try:
        with open(INDEXNOW_KEY_FILE, "w") as f:
            f.write(str(uuid.uuid4()))
    except Exception as e:
        log.warning("Unable to create IndexNow key: %s", e)
try:
    with open(INDEXNOW_KEY_FILE, "r") as f:
        INDEXNOW_KEY = f.read().strip()
except Exception:
    INDEXNOW_KEY = ""

def _ensure_indexnow_verification():
    # Create {key}.txt at site root so Bing can verify ownership
    if not INDEXNOW_KEY:
        return
    vf = os.path.join(REPO_PATH, f"{INDEXNOW_KEY}.txt")
    try:
        if not os.path.exists(vf):
            with open(vf, "w", encoding="utf-8") as f:
                f.write(INDEXNOW_KEY)
    except Exception as e:
        log.warning("Unable to write IndexNow verification file: %s", e)

_ensure_indexnow_verification()

# ---------- Templates ----------
VIDEO_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title | truncate(59) }}</title>
<meta name="description" content="{{ short_desc | truncate(135) }}">
<meta name="generator" content="Mayhem Maker {{ app_version }} ({{ build_stamp }})">
<link rel="canonical" href="{{ site_url }}/videos/{{ slug }}.html">
<link rel="icon" href="/images/favicon.ico">

<!-- Preconnect / DNS -->
<link rel="preconnect" href="https://www.youtube.com">
<link rel="preconnect" href="https://i.ytimg.com">
<link rel="dns-prefetch" href="https://www.youtube.com">
<link rel="dns-prefetch" href="https://i.ytimg.com">

<!-- Open Graph -->
<meta property="og:type" content="video.other">
<meta property="og:title" content="{{ title | truncate(55) }}">
<meta property="og:site_name" content="Martocci Mayhem">
<meta property="og:description" content="{{ short_desc | truncate(60) }}">
<meta property="og:url" content="{{ site_url }}/videos/{{ slug }}.html">
<meta property="og:image" content="https://i.ytimg.com/vi/{{ video_id }}/hqdefault.jpg">

<!-- Twitter -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{{ title | truncate(50) }}">
<meta name="twitter:site" content="@MartocciMayhem">
<meta name="twitter:description" content="{{ short_desc | truncate(120) }}">
<meta name="twitter:image:alt" content="Martocci Mayhem {{ title }}">
<meta name="twitter:image" content="https://i.ytimg.com/vi/{{ video_id }}/hqdefault.jpg">

<style>{% raw %}
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  html, body { overflow-x: hidden; }
  body {
    margin: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
    background: #0f0f0f; color: #fff;
    -webkit-text-size-adjust: 100%;
  }
  img, svg, video, canvas, iframe { display:block; }

  /* Header: logo left, H1 centered (no overlap on mobile) */
  header {
    position: relative;
    display:flex; align-items:center; gap:10px; padding:10px 12px;
    border-bottom:1px solid #222; background:#0f0f0f;
  }
  header .brand {
    display:flex; align-items:center; gap:8px; color:#fff; text-decoration:none; font-weight:700; z-index:2;
  }
  header .brand img { width:28px; height:28px; }
  header .page-title {
    position:absolute; left:50%; transform:translateX(-50%);
    margin:0; font-weight:800; color:#eee;
    font-size:clamp(1rem, 2.2vw, 1.35rem); line-height:1.2; text-align:center;
  }
  @media (max-width:560px){
    header { flex-direction:column; align-items:flex-start; }
    header .page-title { position:static; transform:none; width:100%; text-align:center; margin-top:2px; }
  }

  /* Full-width content */
  main {
    width:100%; margin:0; padding:clamp(12px, 2vw, 24px); max-width:none;
    display:grid; grid-template-columns:minmax(0, 2.6fr) minmax(280px, 1fr);
    gap:clamp(14px, 2vw, 28px);
  }
  @media (max-width:1024px) { main { grid-template-columns: 1fr; } }

  /* Video wrapper reserves height to prevent CLS */
  .video {
        position: relative;
        width: 100%;
        aspect-ratio: 16 / 9;
        overflow: hidden;
        border-radius: 10px;
        border: 1px solid #222;
        box-shadow: 0 0 0 1px #111 inset, 0 10px 25px rgba(0,0,0,.35);
        margin-bottom: 1rem; /* add this line */
    }

  .video iframe { position:absolute; inset:0; width:100%; height:100%; border:0; }
  @supports not (aspect-ratio: 16/9) {
    .video { height: 0; padding-top: 56.25%; }
    .video iframe { position:absolute; inset:0; width:100%; height:100%; }
  }

  .card { background:#111; border:1px solid #222; border-radius:12px; padding:clamp(12px,1.6vw,18px); }
  section .card + .card { margin-top: 16px; }

  .meta a { color:#bbb; margin-right:14px; text-decoration:none; border-bottom:1px dotted #444; }
  .meta a:hover { color:#fff; border-color:#3ea6ff; }
  a, .link { color:#3ea6ff; text-decoration:none; }

  .desc {
    color:#ddd;
    line-height:1.65;
    white-space:pre-wrap;
    overflow-wrap:break-word;
    word-break:normal;
  }

  /* make long links wrap without breaking ASCII art blocks */
  .desc a { word-break: break-all; }

  /* for box-drawing / ASCII lines */
  .desc pre.ascii {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    white-space: pre;         /* never wrap */
    overflow: auto;
    max-width: 100%;
    background:#0c0c0c;
    border:1px solid #222;
    border-radius:8px;
    padding:8px 10px;
    margin:8px 0;
  }

  details summary { cursor:pointer; }

  .block-title { font-weight:800; margin:.2rem 0 .6rem; }
  .subtle { color:#9aa; }

  /* Recommended uses the same grid card layout as index */
  .grid { display:grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap:12px; }
  .rec-card { background:#111; border:1px solid #222; border-radius:12px; overflow:hidden; display:block; }
  .thumb { aspect-ratio:16/9; object-fit:cover; display:block; }
  @media (min-width:550px) { .thumb { width: 100%; } }
  .body { padding:10px 12px; }
  .title { font-weight:800; margin-bottom:6px; }
  .mini { color:#9aa; font-size:1rem; }
  .mini-meta { margin-top:6px; font-size:1rem; }

  footer { padding:16px; text-align:center; border-top:1px solid #222; color:#aaa }
{% endraw %}</style>

<!-- Organization + WebSite (sitewide graph) -->
<script type="application/ld+json">
{{ {
  "@context":"https://schema.org",
  "@graph":[
    {
      "@type":"Organization",
      "@id": site_url + "/#org",
      "name":"Martocci Mayhem",
      "url": site_url,
      "logo":{
        "@type":"ImageObject",
        "url": site_url + "/images/JasonMartocciLogo.webp"
      },
      "sameAs":[
        "https://x.com/MartocciMayhem",
        "https://www.tiktok.com/@MartocciMayhem",
        "https://www.youtube.com/@MartocciMayhem",
        "https://www.instagram.com/MartocciMayhem",
        "https://www.facebook.com/MartocciMayhem"
      ]
    },
    {
        "@type":"WebSite",
        "@id": site_url + "/#website",
        "url": site_url,
        "name":"Martocci Mayhem",
        "publisher":{"@id": site_url + "/#org"},
        "inLanguage":"en"

    }
  ]
} | tojson | replace("</","<\\/") | safe }}
</script>

<!-- VideoObject -->
<script type="application/ld+json">
{{ video_schema | tojson | replace("</","<\\/") | safe }}
</script>

<!-- WebPage (the actual watch page container) -->
<script type="application/ld+json">
{{ {
  "@context":"https://schema.org",
  "@type":"WebPage",
  "@id": site_url + "/videos/" + slug + ".html#webpage",
  "url": site_url + "/videos/" + slug + ".html",
  "name": title,
  "description": short_desc,
  "isPartOf": {"@id": site_url + "/#website"},
  "about": {"@id": site_url + "/#org"},
  "primaryImageOfPage": {
    "@type":"ImageObject",
    "url":"https://i.ytimg.com/vi/" ~ video_id ~ "/hqdefault.jpg"
  },
  "breadcrumb": {"@id": site_url + "/videos/" + slug + ".html#breadcrumb"},
  "datePublished": upload_date,
  "inLanguage": "en"
} | tojson | replace("</","<\\/") | safe }}
</script>

<!-- BreadcrumbList -->
<script type="application/ld+json">
{{ {
  "@context":"https://schema.org",
  "@type":"BreadcrumbList",
  "@id": site_url + "/videos/" + slug + ".html#breadcrumb",
  "itemListElement":[
    {"@type":"ListItem","position":1,"name":"Home","item": site_url},
    {"@type":"ListItem","position":2,"name": title,"item": site_url + "/videos/" + slug + ".html"}
  ]
} | tojson | replace("</","<\\/") | safe }}
</script>

<!-- FAQPage (only include if FAQs are visibly on the page) -->
{% if faq_schema %}
<script type="application/ld+json">
{{ faq_schema | tojson | replace("</","<\\/") | safe }}
</script>
{% endif %}
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('consent', 'default', {
    ad_storage: 'denied',
    analytics_storage: 'denied'
  });
</script>

<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-M23LKR14B1"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-M23LKR14B1');
</script>
</head>
<body>
<header>
  <a class="brand" href="{{ site_url }}">
    <img src="/images/JasonMartocciLogo.webp" width="28" height="28" alt="Logo">
    <span>Martocci Mayhem</span>
  </a>
</header>

<main>
  <section>
    <!-- Title + Stats (above the video) -->
    <div class="card" id="video-meta">
        <h1 class="page-title h2">{{ title | truncate(60) }}</h1>

        <div class="meta" style="margin-top:8px; display:flex; gap:12px; flex-wrap:wrap;">
        <span>Views: {{ view_count }}</span>
        <span>Likes: {{ like_count }}</span>
        <span>
            Channel:
            <a href="https://www.youtube.com/@{{ channel_handle }}" target="_blank" rel="noopener">
            {{ channel_title or ("@" ~ channel_handle) }}
            </a>Subscribers: {{ subs_known and subscriber_count or "—" }}
        </span>
        </div>

        <p style="margin:.4rem 0 0">
        <a class="link" href="https://www.youtube.com/@{{ channel_handle }}?sub_confirmation=1" target="_blank" rel="noopener">Subscribe</a>
        &nbsp;|&nbsp;
        <a class="link" href="https://www.youtube.com/watch?v={{ video_id }}" target="_blank" rel="noopener">Watch on YouTube</a>
        </p>
    </div>

    <div class="card">
        <!-- PLAYER -->
        <div class="video" style="position:relative;width:100%;margin:auto;margin-bottom:12px;aspect-ratio:16/9;overflow:hidden;border-radius:10px;border:1px solid #222;box-shadow:0 0 0 1px #111 inset, 0 10px 25px rgba(0,0,0,.35);">
            <div id="player" role="region" aria-label="Video player: {{ title }}" style="position:absolute;inset:0;"></div>
        </div>
            <!-- CONTROLS (outside the .video box) -->
            <div id="player-controls" style="margin:12px auto 0;position:relative;z-index:1;">
            <div id="yt-controls" style="display:none;gap:8px;flex-wrap:wrap;">
                <button id="play"       type="button" aria-label="Play video">Play</button>
                <button id="pause"      type="button" aria-label="Pause video">Pause</button>
                <button id="mute"       type="button" aria-label="Mute video">Mute</button>
                <button id="unmute"     type="button" aria-label="Unmute video">Unmute</button>
                <button id="fullscreen" type="button" aria-label="Enter fullscreen">Fullscreen</button>
            </div>

            <!-- Fallback if API is blocked -->
            <div id="yt-fallback" style="display:none;margin-top:12px;">
                <a id="yt-link" href="https://www.youtube.com/watch?v={{ video_id }}" target="_blank" rel="noopener">Watch on YouTube</a>
            </div>
        </div>
        <!-- YouTube IFrame API -->
        <script src="https://www.youtube.com/iframe_api"></script>

        <script>
            const VIDEO_ID = "{{ video_id }}";
            const ORIGIN = (location.origin && location.origin.startsWith('http')) ? location.origin : "{{ site_url }}";
            let ytPlayer;

            function showFallback(){
                const fb = document.getElementById('yt-fallback');
                const ctrls = document.getElementById('yt-controls');
                if (ctrls) ctrls.style.display = 'none';
                if (fb) fb.style.display = 'block';
                const link = document.getElementById('yt-link');
                if (link) link.href = "https://www.youtube.com/watch?v=" + VIDEO_ID;
            }

            function setIframeA11yTitle(text) {
                const iframe = document.querySelector('#player iframe');
                if (!iframe) { setTimeout(() => setIframeA11yTitle(text), 60); return; }
                try {
                iframe.setAttribute('title', text);
                iframe.setAttribute('name', text);
                iframe.setAttribute('allowfullscreen', '');
                const allow = iframe.getAttribute('allow') || '';
                if (!/\bfullscreen\b/.test(allow)) {
                    iframe.setAttribute('allow', (allow ? allow + '; ' : '') + 'fullscreen');
                }
                iframe.setAttribute('loading', 'lazy');
                } catch(e) {}
            }

            function ensureFullscreenAllowed() {
                const iframe = document.querySelector('#player iframe');
                if (!iframe) return;
                iframe.setAttribute('allowfullscreen', '');
                const allow = iframe.getAttribute('allow') || '';
                if (!/\bfullscreen\b/.test(allow)) {
                iframe.setAttribute('allow', (allow ? allow + '; ' : '') + 'fullscreen');
                }
            }

            function setFSLabel(entering) {
                const fsBtn = document.getElementById('fullscreen');
                if (!fsBtn) return;
                fsBtn.textContent = entering ? 'Exit Fullscreen' : 'Fullscreen';
                fsBtn.setAttribute('aria-label', fsBtn.textContent);
            }
        </script>

        <script>
            function toggleFullscreen() {
                const el = document.querySelector('#player iframe') || document.getElementById('player');
                if (!el) return;

                const enter = () => {
                // ensure iframe is allowed to go fullscreen
                ensureFullscreenAllowed();
                if (el.requestFullscreen) return el.requestFullscreen();
                if (el.webkitRequestFullscreen) return el.webkitRequestFullscreen(); // Safari
                if (el.msRequestFullscreen) return el.msRequestFullscreen();         // old Edge
                return Promise.resolve();
                };

                const exit = () => {
                if (document.exitFullscreen) return document.exitFullscreen();
                if (document.webkitExitFullscreen) return document.webkitExitFullscreen(); // Safari
                if (document.msExitFullscreen) return document.msExitFullscreen();         // old Edge
                return Promise.resolve();
                };

                if (!document.fullscreenElement && !document.webkitFullscreenElement) {
                enter().then(() => setFSLabel(true)).catch(() => {});
                } else {
                exit().then(() => setFSLabel(false)).catch(() => {});
                }
            }
        </script>
        <script>
            // Keep the button label in sync if the user exits with ESC, etc.
            document.addEventListener('fullscreenchange', () => {
                setFSLabel(!!document.fullscreenElement);
            });

            // Wire button events once the player is ready
            function wireControls() {
                const play   = document.getElementById('play');
                const pause  = document.getElementById('pause');
                const mute   = document.getElementById('mute');
                const unmute = document.getElementById('unmute');
                const fs     = document.getElementById('fullscreen');

                if (play)   play.onclick   = () => ytPlayer && ytPlayer.playVideo();
                if (pause)  pause.onclick  = () => ytPlayer && ytPlayer.pauseVideo();
                if (mute)   mute.onclick   = () => ytPlayer && ytPlayer.mute();
                if (unmute) unmute.onclick = () => ytPlayer && ytPlayer.unMute();
                if (fs)     fs.onclick     = toggleFullscreen;
            }

            // Bootstrap the YouTube IFrame API
            window.onYouTubeIframeAPIReady = function () {
                try {
                ytPlayer = new YT.Player('player', {
                    host: 'https://www.youtube.com',
                    videoId: VIDEO_ID,
                    playerVars: {
                    enablejsapi: 1,
                    origin: ORIGIN,   // must match https://MartocciMayhem.com in production
                    rel: 0,
                    modestbranding: 1,
                    playsinline: 1
                    },
                    events: {
                    onReady: function () {
                        setIframeA11yTitle('YouTube video: {{ title }}');
                        ensureFullscreenAllowed();
                        wireControls();
                        const ctrls = document.getElementById('yt-controls');
                        if (ctrls) ctrls.style.display = 'flex';
                        setFSLabel(false);
                    },
                    onError: function () {
                        showFallback();
                    }
                    }
                });
                } catch (e) {
                showFallback();
                }
            };
        </script> 
        <script>
            document.addEventListener('webkitfullscreenchange', () => {
                setFSLabel(!!(document.fullscreenElement || document.webkitFullscreenElement));
            });
        </script>
    </div>
    <!-- Description -->
    <div class="card">
      <h2 class="block-title">Description</h2>
      <div class="desc">{{ formatted_desc | safe }}</div>
    </div>

    <!-- FAQ (visual) -->
    {% if faqs and faqs|length > 0 %}
      <div class="card faq" id="faq">
        <h2 class="block-title">Frequently Asked Questions</h2>
        {% for q,a in faqs %}
          <details style="margin-bottom:.6rem; border:1px solid #222; border-radius:8px; background:#0c0c0c; padding:.4rem .8rem;">
            <summary>{{ q }}</summary>
            <div class="answer" style="padding:.6rem;color:#ccc">
              <div>{{ a|safe }}</div>
            </div>
          </details>
        {% endfor %}
      </div>
    {% endif %}

    <!-- Comments -->
    <div class="card">
      <h2 class="block-title">Comments</h2>
      <p class="subtle">Join the discussion on YouTube to see real-time comments, likes, and replies.</p>
      <p><a class="link" href="https://www.youtube.com/watch?v={{ video_id }}#comments" target="_blank" rel="noopener">Open comments on YouTube</a></p>
    </div>
  </section>

  <!-- Sidebar / Recommended -->
  <aside class="rec">
    <div class="card">
      <h2 class="block-title">Recommended Videos</h2>
      {% if related and related|length > 0 %}
      <div class="grid">
        {% for r in related %}
          <a class="rec-card" href="/videos/{{ r.slug }}.html">
            <img class="thumb" decoding="async" loading="lazy"
                 src="https://i.ytimg.com/vi/{{ r.video_id }}/hqdefault.jpg"
                 width="320" height="180"
                 alt="{{ r.title }}"
                 onerror="this.onerror=null;this.src='/images/default-thumbnail.png'">
            <div class="body">
              <div class="title">{{ r.title }}</div>
              <div class="mini">{{ r.desc | truncate(90) }}</div>
              <div class="mini-meta">Views: {{ r.view_count }}</div>
            </div>
          </a>
        {% endfor %}
      </div>
      {% else %}
        <div class="subtle">More suggestions coming soon.</div>
      {% endif %}
    </div>
  </aside>
</main>

<footer>
  <div class="footer-inner">
    <div>© {{ current_year }} Martocci Mayhem</div>

    <style>
      .socials {
        display: flex;
        align-items: center;
        gap: .6rem;
        flex-wrap: wrap
      }
      .social-btn {
        width: 40px;
        height: 40px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,.25);
        color: #fff;
        background: transparent;
        text-decoration: none;
        transition: transform .12s ease, background .12s ease
      }
      .social-btn svg {
        width: 20px;
        height: 20px;
        display: block;
        fill: currentColor
      }
      .social-btn:hover {
        background: rgba(255,255,255,.1);
        transform: translateY(-1px)
      }
      .social-btn:focus-visible {
        outline: 2px solid currentColor;
        outline-offset: 2px
      }
    </style>

    <nav class="socials" aria-label="Social links">
      <!-- X / Twitter -->
      <a class="social-btn" href="https://x.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="X (Twitter)" title="X (Twitter)">
        <svg viewBox="0 0 1200 1227" aria-hidden="true">
          <path d="M714 519 1160 0H1064L663 464 357 0H0l463 681L0 1227h96l424-483 330 483h357L714 519ZM556 676l-49-70-389-553h167l314 445 49 70 401 569h-167L556 676Z"/>
        </svg>
      </a>

      <!-- TikTok -->
      <a class="social-btn" href="https://www.tiktok.com/@MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="TikTok" title="TikTok">
        <svg viewBox="0 0 48 48" aria-hidden="true">
          <path d="M41,18.6c-3.4,0-6.6-1.1-9.2-3.1v12.4c0,7.8-6.3,14.1-14.1,14.1S3.5,35.7,3.5,27.9c0-6.3,4.1-11.6,9.8-13.4v6.3 c-2.1,1.3-3.5,3.7-3.5,6.4c0,4.1,3.4,7.5,7.5,7.5s7.5-3.4,7.5-7.5V6.5h6.4c0.6,3.6,3.4,6.6,7,7.5V18.6z"/>
        </svg>
      </a>

      <!-- YouTube -->
      <a class="social-btn" href="https://www.youtube.com/@MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="YouTube" title="YouTube">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M23.498 6.186a2.974 2.974 0 0 0-2.093-2.103C19.505 3.5 12 3.5 12 3.5s-7.505 0-9.405.583a2.974 2.974 0 0 0-2.093 2.103C0 8.095 0 12 0 12s0 3.905.502 5.814a2.974 2.974 0 0 0 2.093 2.103C4.495 20.5 12 20.5 12 20.5s7.505 0 9.405-.583a2.974 2.974 0 0 0 2.093-2.103C24 15.905 24 12 24 12s0-3.905-.502-5.814ZM9.75 15.5v-7l6.5 3.5-6.5 3.5Z"/>
        </svg>
      </a>

      <!-- Instagram -->
      <a class="social-btn" href="https://www.instagram.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="Instagram" title="Instagram">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M7.75 2h8.5A5.75 5.75 0 0 1 22 7.75v8.5A5.75 5.75 0 0 1 16.25 22h-8.5A5.75 5.75 0 0 1 2 16.25v-8.5A5.75 5.75 0 0 1 7.75 2ZM12 7a5 5 0 1 0 0 10 5 5 0 0 0 0-10Zm6.25-.75a1.25 1.25 0 1 0 0 2.5 1.25 1.25 0 0 0 0-2.5Z"/>
        </svg>
      </a>

      <!-- Facebook -->
      <a class="social-btn" href="https://www.facebook.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="Facebook" title="Facebook">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M22 12a10 10 0 1 0-11.5 9.9v-7h-2v-3h2v-2.3c0-2 1.2-3.1 3-3.1.9 0 1.8.1 1.8.1v2h-1c-1 0-1.3.6-1.3 1.2V12h2.3l-.4 3h-1.9v7A10 10 0 0 0 22 12"/>
        </svg>
      </a>

      <!-- LinkedIn -->
      <a class="social-btn" href="https://www.linkedin.com/in/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="LinkedIn" title="LinkedIn">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M4.98 3.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5ZM3.5 8.98h2.96V21H3.5zM9 8.98h2.84v1.63h.04c.4-.75 1.38-1.54 2.84-1.54 3.03 0 3.59 1.99 3.59 4.57V21h-2.96v-4.92c0-1.17-.02-2.67-1.63-2.67-1.63 0-1.88 1.27-1.88 2.58V21H9z"/>
        </svg>
      </a>
    </nav>
  </div>
</footer>

<!-- generated by Mayhem Maker v{{ app_version }} @ {{ build_stamp }}Z -->
</body></html>
"""

INDEX_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unlock ASMR Magic & Epic Savage Grandma at Martocci Mayhem</title>
<meta name="description" content="Explore the full Martocci Mayhem catalog: ASMR, comedy, shorts, tech experiments and more.">
<meta name="generator" content="Mayhem Maker {{ app_version }} ({{ build_stamp }})">
<link rel="canonical" href="{{ site_url }}">
<link rel="icon" href="/images/favicon.ico">

<!-- Preconnect / DNS -->
<link rel="preconnect" href="https://i.ytimg.com">
<link rel="dns-prefetch" href="https://i.ytimg.com">

<!-- Open Graph -->
<meta property="og:type" content="website">
<meta property="og:title" content="Unlock ASMR Magic & Epic Savage Grandma at Martocci Mayhem">
<meta property="og:site_name" content="Martocci Mayhem">
<meta property="og:description" content="Explore ASMR, comedy, shorts and tech-heavy experiments.">
<meta property="og:url" content="{{ site_url }}">
<meta property="og:image" content="{{ site_url }}/images/JasonMartocciLogo.webp">

<!-- Twitter -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Unlock ASMR Magic & Epic Savage Grandma">
<meta name="twitter:site" content="@MartocciMayhem">
<meta name="twitter:description" content="Explore ASMR, comedy, shorts and tech-heavy experiments.">
<meta name="twitter:image:alt" content="Unlock ASMR Magic & Epic Savage Grandma at Martocci Mayhem">
<meta name="twitter:image" content="{{ site_url }}/images/JasonMartocciLogo.webp">

<style>{% raw %}
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  html, body { overflow-x: hidden; }
  body {
    margin: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
    background:#0f0f0f; color:#fff;
    -webkit-text-size-adjust: 100%;
  }
  img, svg, video, canvas, iframe { display:block; }

  /* ===== Header (logo left, title centered; no overlap on mobile) ===== */
  header {
    position: relative;
    display: flex; align-items: center;
    gap: 12px; padding: 10px 12px;
    border-bottom:1px solid #222; background:#0f0f0f;
  }
  header .brand {
    display:flex; align-items:center; gap:8px;
    color:#fff; text-decoration:none; font-weight:700; z-index:2;
  }
  header .brand img { width:28px; height:28px; }
  header .site-title{
    position:absolute; left:50%; transform:translateX(-50%);
    margin:0; font-weight:800; color:#eee;
    font-size:clamp(1rem, 2.2vw, 1.35rem); line-height:1.2; text-align:center;
  }
  @media (max-width: 560px){
    header{ flex-direction:column; align-items:flex-start; }
    header .site-title{ position:static; transform:none; width:100%; text-align:center; margin-top:2px; }
  }

  /* ===== Sticky toolbar (only this is sticky) ===== */
  /* One sticky block that contains two stacked rows */
    .toolbar-wrap {
    position: sticky;
    top: 0;                 /* if you later make the header sticky, set this to its height */
    z-index: 20;
    background:#0f0f0f;
    }

    /* Rows */
    .filters-row,
    .search-row {
    padding:10px 12px;
    background:#0f0f0f;
    }

    /* Category chips (horizontal scroll) */
  /* Horizontal scroll filter chips */
  .filters {
    display:flex; flex:1 1 auto; gap:8px; overflow-x:auto; -webkit-overflow-scrolling:touch;
    scrollbar-width:none; padding-bottom:2px;
  }
  .filters::-webkit-scrollbar { display:none; }
  .filters button {
    background:#161616; border:1px solid #222; color:#bbb; padding:8px 12px;
    border-radius:999px; cursor:pointer; white-space:nowrap; flex:0 0 auto;
  }
  .filters button.active { color:#fff; border-color:#3ea6ff; }

  .search { 
    width:100%; padding:10px 12px; border-radius:999px; border:1px solid #222;
    background:#111; color:#ddd; }
  .search input {
    width:100%; padding:10px 12px; border-radius:999px; border:1px solid #222;
    background:#111; color:#ddd;
  }

  /* ===== Full-width content ===== */
  main { width:100%; margin:0; padding:12px; max-width:none; }
  .grid { display:grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap:16px; }

  .card { background:#111; border:1px solid #222; border-radius:12px; overflow:hidden; display:block; }
  .thumb { aspect-ratio:16/9; object-fit:cover; display:block;}
  .body { padding:10px 12px; }
  a, .link { color:#3ea6ff; text-decoration:none; }
  .title { font-weight:800; margin:8px 0 6px; }
  .cat { color:#9aa; font-size:1rem; }
  .desc { color:#9aa; margin-top:6px; }
  .meta { margin-top:8px; font-size:1rem; }

  /* ===== Footer with social buttons ===== */
  footer { padding:18px 12px; border-top:1px solid #222; color:#aaa; background:#0f0f0f; }
  .footer-inner { max-width:1400px; margin:0 auto; display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px; }
  .socials { display:flex; gap:10px; }
  .social-btn {
    width:40px; height:40px; border-radius:50%; display:grid; place-items:center;
    background:#141414; border:1px solid #222; transition:transform .15s ease, border-color .15s ease;
  }
  .social-btn:hover { transform:translateY(-1px); border-color:#3ea6ff; }
  .social-btn svg { width:20px; height:20px; fill:#cfcfcf; }

  @media (max-width: 560px){
    .toolbar { padding:8px 10px; gap:8px; }
    .search  { flex:0 0 min(60%, 320px); }
  }
{% endraw %}</style>

<!-- JSON-LD (built in Python to avoid fragile loops) -->
<script type="application/ld+json">
{{ item_list_schema | tojson | replace("</","<\\/") | safe }}
</script>

<script type="application/ld+json">
{{ web_page_schema | tojson | replace("</","<\\/") | safe }}
</script>

<script id="__data" type="application/json">
{{ videos | tojson | replace("</","<\\/") | safe }}
</script>

<!-- Organization + WebSite -->
<script type="application/ld+json">
{{ {
  "@context":"https://schema.org",
  "@graph":[
    {
      "@type":"Organization",
      "@id": site_url + "/#org",
      "name":"Martocci Mayhem",
      "url": site_url,
      "logo":{"@type":"ImageObject","url": site_url + "/images/JasonMartocciLogo.webp"},
      "sameAs":[
        "https://x.com/MartocciMayhem",
        "https://www.tiktok.com/@MartocciMayhem",
        "https://www.youtube.com/@MartocciMayhem",
        "https://www.instagram.com/MartocciMayhem",
        "https://www.facebook.com/MartocciMayhem"
      ]
    },
    {
      "@type":"WebSite",
      "@id": site_url + "/#website",
      "url": site_url,
      "name":"Martocci Mayhem",
      "publisher":{"@id": site_url + "/#org"},
      "inLanguage":"en"
    }
  ]
} | tojson | replace("</","<\\/") | safe }}
</script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('consent', 'default', {
    ad_storage: 'denied',
    analytics_storage: 'denied'
  });
</script>

<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-M23LKR14B1"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-M23LKR14B1');
</script>
</head>
<body>
<header>
  <a class="brand" href="{{ site_url }}"><img src="/images/JasonMartocciLogo.webp" width="28" height="28" alt="Logo"><span>Martocci Mayhem</span></a>
  <h1 class="site-title">Slice • Smash • Laugh • Repeat 🔥</h1>
</header>

<main>
  <div class="toolbar-wrap">
    <div class="filters-row">
      <div class="filters" id="filters"></div>
    </div>
    <div class="search-row">
      <input id="q" class="search" type="search" placeholder="Search titles, descriptions, tags…">
    </div>
  </div>
  <!-- SINGLE grid -->
  <div class="grid" id="grid"></div>

  <!-- Sentinel should immediately follow the grid it drives -->
  <div id="sentinel" aria-hidden="true"></div>
</main>

<footer>
  <div class="footer-inner">
    <div>© {{ current_year }} Martocci Mayhem</div>

    <style>
      .socials {
        display: flex;
        align-items: center;
        gap: .6rem;
        flex-wrap: wrap
      }
      .social-btn {
        width: 40px;
        height: 40px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,.25);
        color: #fff;
        background: transparent;
        text-decoration: none;
        transition: transform .12s ease, background .12s ease
      }
      .social-btn svg {
        width: 20px;
        height: 20px;
        display: block;
        fill: currentColor
      }
      .social-btn:hover {
        background: rgba(255,255,255,.1);
        transform: translateY(-1px)
      }
      .social-btn:focus-visible {
        outline: 2px solid currentColor;
        outline-offset: 2px
      }
    </style>

    <nav class="socials" aria-label="Social links">
      <!-- X / Twitter -->
      <a class="social-btn" href="https://x.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="X (Twitter)" title="X (Twitter)">
        <svg viewBox="0 0 1200 1227" aria-hidden="true">
          <path d="M714 519 1160 0H1064L663 464 357 0H0l463 681L0 1227h96l424-483 330 483h357L714 519ZM556 676l-49-70-389-553h167l314 445 49 70 401 569h-167L556 676Z"/>
        </svg>
      </a>

      <!-- TikTok -->
      <a class="social-btn" href="https://www.tiktok.com/@MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="TikTok" title="TikTok">
        <svg viewBox="0 0 48 48" aria-hidden="true">
          <path d="M41,18.6c-3.4,0-6.6-1.1-9.2-3.1v12.4c0,7.8-6.3,14.1-14.1,14.1S3.5,35.7,3.5,27.9c0-6.3,4.1-11.6,9.8-13.4v6.3 c-2.1,1.3-3.5,3.7-3.5,6.4c0,4.1,3.4,7.5,7.5,7.5s7.5-3.4,7.5-7.5V6.5h6.4c0.6,3.6,3.4,6.6,7,7.5V18.6z"/>
        </svg>
      </a>

      <!-- YouTube -->
      <a class="social-btn" href="https://www.youtube.com/@MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="YouTube" title="YouTube">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M23.498 6.186a2.974 2.974 0 0 0-2.093-2.103C19.505 3.5 12 3.5 12 3.5s-7.505 0-9.405.583a2.974 2.974 0 0 0-2.093 2.103C0 8.095 0 12 0 12s0 3.905.502 5.814a2.974 2.974 0 0 0 2.093 2.103C4.495 20.5 12 20.5 12 20.5s7.505 0 9.405-.583a2.974 2.974 0 0 0 2.093-2.103C24 15.905 24 12 24 12s0-3.905-.502-5.814ZM9.75 15.5v-7l6.5 3.5-6.5 3.5Z"/>
        </svg>
      </a>

      <!-- Instagram -->
      <a class="social-btn" href="https://www.instagram.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="Instagram" title="Instagram">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M7.75 2h8.5A5.75 5.75 0 0 1 22 7.75v8.5A5.75 5.75 0 0 1 16.25 22h-8.5A5.75 5.75 0 0 1 2 16.25v-8.5A5.75 5.75 0 0 1 7.75 2ZM12 7a5 5 0 1 0 0 10 5 5 0 0 0 0-10Zm6.25-.75a1.25 1.25 0 1 0 0 2.5 1.25 1.25 0 0 0 0-2.5Z"/>
        </svg>
      </a>

      <!-- Facebook -->
      <a class="social-btn" href="https://www.facebook.com/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="Facebook" title="Facebook">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M22 12a10 10 0 1 0-11.5 9.9v-7h-2v-3h2v-2.3c0-2 1.2-3.1 3-3.1.9 0 1.8.1 1.8.1v2h-1c-1 0-1.3.6-1.3 1.2V12h2.3l-.4 3h-1.9v7A10 10 0 0 0 22 12"/>
        </svg>
      </a>

      <!-- LinkedIn -->
      <a class="social-btn" href="https://www.linkedin.com/in/MartocciMayhem" target="_blank" rel="noopener noreferrer" aria-label="LinkedIn" title="LinkedIn">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M4.98 3.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5ZM3.5 8.98h2.96V21H3.5zM9 8.98h2.84v1.63h.04c.4-.75 1.38-1.54 2.84-1.54 3.03 0 3.59 1.99 3.59 4.57V21h-2.96v-4.92c0-1.17-.02-2.67-1.63-2.67-1.63 0-1.88 1.27-1.88 2.58V21H9z"/>
        </svg>
      </a>
    </nav>
  </div>
</footer>

<script>
    "use strict";
    (function(){
    function debounce(fn,ms){ let t; return function(){ const a=arguments; clearTimeout(t); t=setTimeout(()=>fn.apply(null,a), ms||250); }; }
    function compactMeta(views, likes, comments){
        const v = "Views: "  + Number(views||0).toLocaleString();
        const l = "Likes: "  + Number(likes||0).toLocaleString();
        const full = v + " · " + l;
        return (full.length > 42) ? (v + " · " + l) : full;
    }

    const RAW = document.getElementById("__data");
    let ALL = [];
    try { ALL = JSON.parse((RAW && RAW.textContent) || "[]"); } catch(e){ console.error("Bad data JSON", e); }

    const DATA = ALL.map(v => ({...v, _t:(v.title||"").toLowerCase(), _d:(v.desc||"").toLowerCase(), _tags:(v.tags||[]).map(t=>String(t).toLowerCase()) }));
    const cats = ["All", ...Array.from(new Set(DATA.map(v => v.category || "People & Blogs"))).sort((a,b)=>a.localeCompare(b))];

    const filters  = document.getElementById("filters");
    const grid     = document.getElementById("grid");
    const qEl      = document.getElementById("q");
    const sentinel = document.getElementById("sentinel");

    if (!filters || !grid || !qEl || !sentinel) { console.warn("Missing required elements"); return; }

    let currentCat = "All", query = "", filtered = [], pageSize = 30, cursor = 0, observer = null;

    cats.forEach(c=>{
        const b=document.createElement("button");
        b.type="button";
        b.textContent=c;
        b.addEventListener("click", ()=>{ currentCat=c; applyFilters(); });
        if(c===currentCat) b.classList.add("active");
        filters.appendChild(b);
    });

    qEl.addEventListener("input", debounce(e=>{
        query=(e.target.value||"").toLowerCase().trim();
        applyFilters();
    }, 200));

    function matchesQuery(v, q){
        if (!q) return true;
        const parts = q.split(/\s+/);
        return parts.every(tok=>{
        if (tok.startsWith("tag:")) return v._tags.some(x=>x.includes(tok.slice(4)));
        if (tok.startsWith("id:"))  return String(v.video_id||"").toLowerCase().includes(tok.slice(3));
        return v._t.includes(tok)||v._d.includes(tok)||v._tags.some(x=>x.includes(tok));
        });
    }

    function applyFilters(){
        Array.prototype.forEach.call(filters.children, b=>b.classList.toggle("active", b.textContent===currentCat));
        filtered = DATA
        .filter(v => (currentCat==="All" || v.category===currentCat) && matchesQuery(v, query))
        .sort((a,b)=> String(b.last_edited_date||"").localeCompare(String(a.last_edited_date||""))); // ok if ISO dates

        grid.textContent = "";
        cursor = 0;
        if (observer) observer.disconnect();
        loadMore();
        observer = new IntersectionObserver((entries)=>{ if (entries[0].isIntersecting) loadMore(); }, { rootMargin: "1200px" });
        if (sentinel) observer.observe(sentinel);
    }

    function loadMore(){
        if (cursor >= filtered.length) return;
        const end = Math.min(cursor + pageSize, filtered.length);
        const frag = document.createDocumentFragment();
        for (let i=cursor; i<end; i++) frag.appendChild(card(filtered[i]));
        grid.appendChild(frag);
        cursor = end;
    }

    function card(v){
        const a = document.createElement("a");
        a.href = "/videos/" + (v.slug||"") + ".html";
        a.className="card";

        const img = document.createElement("img");
        img.className="thumb";
        img.loading="lazy";
        img.decoding="async";
        img.width=320; img.height=180;
        img.src="https://i.ytimg.com/vi/"+v.video_id+"/hqdefault.jpg";
        img.alt=v.title||"";
        img.onerror=function(){ this.onerror=null; this.src='/images/default-thumbnail.png'; };

        const body=document.createElement("div"); body.className="body";
        const t=document.createElement("div"); t.className="title"; t.textContent=v.title||"";
        const cat=document.createElement("div"); cat.className="cat"; cat.textContent=String(v.category||"");
        const desc=document.createElement("div"); desc.className="desc"; const raw=(v.desc||""); desc.textContent=raw.length>90?(raw.slice(0,90)+"…"):raw;
        const meta=document.createElement("div"); meta.className="meta"; meta.textContent=compactMeta(v.view_count, v.like_count, v.comment_count);

        body.appendChild(t); body.appendChild(cat); body.appendChild(desc); body.appendChild(meta);
        a.appendChild(img); a.appendChild(body);
        return a;
    }

    applyFilters();
    })();
</script>
<!-- generated by Mayhem Maker v{{ app_version }} @ {{ build_stamp }}Z -->
</body></html>
"""

def ensure_templates(force=True):
    import shutil
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
    mapping = {
        "video_template.html": VIDEO_TEMPLATE,
        "index_template.html": INDEX_TEMPLATE,
    }
    for name, content in mapping.items():
        path = os.path.join(TEMPLATE_DIR, name)
        if force and os.path.exists(path):
            try: shutil.move(path, path + ".bak")
            except Exception: pass
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

def regenerate_sitemap(videos:list[dict]):
    sm = ["""<?xml version="1.0" encoding="UTF-8"?>""",
          """<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">""",
          f"""  <url><loc>{SITE_URL}</loc><lastmod>{iso_date()}</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>"""]
    for v in videos:
        url = f"{SITE_URL}/videos/{output_basename(v)}.html"
        lastmod = v.get("last_edited_date") or v.get("creation_date") or iso_date()
        sm.append(f"""  <url><loc>{url}</loc><lastmod>{lastmod}</lastmod><changefreq>weekly</changefreq><priority>0.8</priority></url>""")
    sm.append("</urlset>")
    return os.path.join(REPO_PATH, "sitemap.xml"), "\n".join(sm)

# ---------- Deploy ----------

def repo_has_changes(repo) -> bool:
    return bool(repo.git.status("--porcelain").strip())

def deploy(commit_msg: str, ping_urls: list[str] | None = None, prog=None) -> str:
    """
    Add/commit/push the repo, then notify search engines.
    Progress mapping inside deploy:
      0.00–0.10   open repo, add, status
      0.10–0.30   commit
      0.30–0.70   push
      0.70–0.82   settle/no-op
      0.82–0.92   IndexNow
      0.92–0.98   Google ping
      0.98–1.00   wrap up
    """
    last = [0.0]
    def _p(frac, msg):
        try:
            if prog:
                f = max(0.0, min(1.0, float(frac)))
                if f < last[0]:
                    f = last[0]
                last[0] = f
                prog(f, str(msg))
        except Exception:
            pass

    out_lines: list[str] = []
    try:
        _p(0.02, "Opening repo")
        try:
            repo = Repo(REPO_PATH)
        except Exception as e:
            return f"Deploy failed: cannot open repo at {REPO_PATH}\n{e}"

        _p(0.06, "git add -A")
        try:
            repo.git.add(A=True)
        except Exception as e:
            out_lines.append(f"git add failed: {e}")

        _p(0.10, "Checking status")
        dirty = repo_has_changes(repo)
        if dirty:
            _p(0.18, "Committing Now")
            try:
                repo.index.commit(commit_msg or "Update site")
                out_lines.append("Committed changes.")
            except Exception as e:
                out_lines.append(f"Commit failed: {e}")
        else:
            out_lines.append("No changes to commit.")

        _p(0.30, "Checking remote")
        try:
            try:
                origin = repo.remote(name="origin")
            except Exception:
                origin = next(iter(repo.remotes), None)
        except Exception:
            origin = None

        if origin:
            _p(0.40, f"Pushing to {origin.name.capitalize()}")
            try:
                origin = repo.remotes.origin
                # NEW: make sure we’re up to date first
                origin.pull()  # or: repo.remotes.origin.pull('main')
                out_lines.append("Pulled latest from origin before pushing.")
            except Exception as e:
                out_lines.append(f"Pull failed: {e}")
                # You can choose to return here, or keep going to attempt push

            try:
                origin.push()
                out_lines.append(f"Pushed to {origin.name}.")
            except Exception as e:
                out_lines.append(f"Push failed: {e}")
        else:
            out_lines.append("No remote configured; skipped push.")

        # IndexNow 0.82..0.92
        _p(0.82, "IndexNow")
        out_lines.append(indexnow_submit(
            ping_urls or [],
            prog=lambda r, m: _p(0.82 + 0.10 * max(0.0, min(1.0, float(r))), m)
        ))

        # Google ping 0.92..0.98
        _p(0.92, "Google ping")
        out_lines.append(google_sitemap_ping(
            prog=lambda r, m: _p(0.92 + 0.06 * max(0.0, min(1.0, float(r))), m)
        ))

        return "\n".join([s for s in out_lines if s])
    finally:
        _p(1.0, "done")

def indexnow_submit(urls: list[str], prog=None) -> str:
    if "requests" in [m[0] for m in _missing]:
        return ""
    if not SET.get("ENABLE_INDEXNOW", True) or not INDEXNOW_KEY or not urls:
        # still “finish” the sub-progress if we were asked to track it
        try:
            if prog: prog(1.0, "IndexNow skipped")
        except Exception:
            pass
        return ""

    host = urlsplit(SITE_URL).netloc
    keyloc = f"{SITE_URL}/{INDEXNOW_KEY}.txt"

    total = max(1, len(urls))
    out_lines = []
    for i, u in enumerate(urls, 1):
        try:
            r = requests.get(
                "https://www.bing.com/indexnow",
                params={"url": u, "key": INDEXNOW_KEY, "keyLocation": keyloc, "host": host},
                timeout=8
            )
            out_lines.append(f"IndexNow: {u} -> {r.status_code}")
        except Exception as e:
            out_lines.append(f"IndexNow failed for {u}: {e}")
        # progress: i / total ∈ (0,1], monotonic
        try:
            if prog: prog(i / total, f"Pushing to IndexNow {i}/{total}")
        except Exception:
            pass

    return "\n".join(out_lines) + ("\n" if out_lines else "")

def google_sitemap_ping(prog=None) -> str:
    if "requests" in [m[0] for m in _missing]:
        if prog:
            try: prog(1.0, "Google ping skipped (requests missing)")
            except Exception: pass
        return ""
    try:
        if prog:
            try:
                # quick trickle while waiting
                stop = {"flag": False}
                def _trickle():
                    t = 0.0
                    while not stop["flag"] and t < 0.85:
                        t = min(0.85, t + 0.05)
                        prog(t, "Google ping…")
                        time.sleep(0.25)
                th = threading.Thread(target=_trickle, daemon=True); th.start()
            except Exception:
                th = None
        sm = f"{SITE_URL}/sitemap.xml"
        r  = requests.get("https://www.google.com/ping", params={"sitemap": sm}, timeout=10)
        return f"Google ping: {r.status_code}\n"
    except Exception as e:
        return f"Google ping failed: {e}\n"
    finally:
        try:
            stop["flag"] = True
            if 'th' in locals() and th: th.join(timeout=0.05)
            if prog: prog(1.0, "Google ping done")
        except Exception:
            pass

# ---------- API refresh ----------
def parse_iso8601_duration(s:str) -> int:
    if not s or not s.startswith("PT"): return 0
    h=m=sec=0
    mH = re.search(r"(\d+)H", s); mM = re.search(r"(\d+)M", s); mS = re.search(r"(\d+)S", s)
    if mH: h=int(mH.group(1))
    if mM: m=int(mM.group(1))
    if mS: sec=int(mS.group(1))
    return h*3600 + m*60 + sec

def refresh_stale(days:int|None=None, by_ids:set[str]|None=None):
    if "requests" in [m[0] for m in _missing]: raise RuntimeError("requests not installed")
    days = int(days if days is not None else SET["SMART_REFRESH_DAYS"])
    cutoff = utcnow() - dt.timedelta(days=days)
    videos = load_cache()
    removed = load_removed()
    ch = fetch_channel_details()
    updated, errors = 0, []
    for i, v in enumerate(videos):
        if v.get("video_id") in removed:
            continue
        if by_ids and v["video_id"] not in by_ids:
            continue
        last_fetch = v.get("fetched_at")
        needs = bool(by_ids) or (not last_fetch)
        if not needs and last_fetch:
            try:
                ts = dt.datetime.fromisoformat(last_fetch.replace("Z","+00:00"))
                needs = ts < cutoff
            except Exception:
                needs = True
        if not needs: continue
        try:
            details, new_etag = fetch_video_details(v["video_id"], v.get("etag"))
            if details:
                sn = details["snippet"]; st = details.get("statistics", {}); cd = details.get("contentDetails", {})
                slug = v.get("slug") or slugify(sn["title"])
                duration = parse_iso8601_duration(cd.get("duration",""))
                v.update({
                    "title": sn.get("title",""), "desc": sn.get("description",""), "tags": sn.get("tags", []),
                    "slug": slug, "creation_date": sn.get("publishedAt","")[:10],
                    "last_edited_date": sn.get("publishedAt", "")[:10],
                    "view_count": int(st.get("viewCount", 0) or 0), "like_count": int(st.get("likeCount", 0) or 0),
                    "comment_count": int(st.get("commentCount", 0) or 0), "category": CATEGORIES.get(sn.get("categoryId","22"), "People & Blogs"),
                    "duration_seconds": duration, "channel_id": sn.get("channelId",""), "channel_title": ch["title"], "subs": ch["subs"],
                    "etag": new_etag
                })
            v["fetched_at"] = utcnow().isoformat().replace("+00:00","Z")
            videos[i] = v
            updated += 1
        except Exception as e:
            errors.append(f"{v['video_id']}: {e}")
    save_cache(videos)
    return updated, errors

def fetch_latest(days: int = 7):
    pub_after = (utcnow() - dt.timedelta(days=int(days))).isoformat().replace("+00:00", "Z")
    added, errors = 0, []
    videos = load_cache()
    ids_in_cache = {v["video_id"] for v in videos}
    removed = load_removed()

    try:
        channel = fetch_channel_details()
    except QuotaExceeded as e:
        return added, errors + [str(e) + " (channels.list)"]

    try:
        data = fetch_list_by_channel(published_after=pub_after)
    except QuotaExceeded as e:
        return added, errors + [str(e) + " (search.list)"]

    while True:
        for item in data.get("items", []):
            if item["id"]["kind"] != "youtube#video":
                continue
            vid = item["id"]["videoId"]
            if vid in ids_in_cache or vid in removed:
                continue
            try:
                details, etag = fetch_video_details(vid)
                if not details:
                    continue
                sn = details["snippet"]; st = details.get("statistics", {})
                cd = details.get("contentDetails", {})
                duration = parse_iso8601_duration(cd.get("duration", ""))

                videos.append({
                    "video_id": vid,
                    "title": sn.get("title", ""),
                    "desc": sn.get("description", ""),
                    "tags": sn.get("tags", []),
                    "slug": slugify(sn.get("title", "")),
                    "creation_date": sn.get("publishedAt", "")[:10],
                    "last_edited_date": sn.get("publishedAt", "")[:10],
                    "view_count": int(st.get("viewCount", 0) or 0),
                    "like_count": int(st.get("likeCount", 0) or 0),
                    "comment_count": int(st.get("commentCount", 0) or 0),
                    "category": CATEGORIES.get(sn.get("categoryId", "22"), "People & Blogs"),
                    "duration_seconds": duration,
                    "channel_id": sn.get("channelId", ""),
                    "channel_title": channel["title"],
                    "subs": channel["subs"],
                    "etag": etag,
                    "fetched_at": utcnow().isoformat().replace("+00:00", "Z")
                })
                ids_in_cache.add(vid); added += 1
            except QuotaExceeded as e:
                errors.append(str(e) + f" while fetching video {vid}")
                save_cache(videos)
                return added, errors
            except Exception as e:
                errors.append(f"{vid}: {e}")

        next_page = data.get("nextPageToken")
        if not next_page:
            break
        if remaining_quota() < API_COSTS.get("search.list", 100):
            errors.append("Stopped early to conserve quota.")
            break
        try:
            data = fetch_list_by_channel(page_token=next_page, published_after=pub_after)
        except QuotaExceeded as e:
            errors.append(str(e) + " during pagination")
            break

    save_cache(videos)
    return added, errors

def fetch_all(max_pages=None):
    added, errors = 0, []
    videos = load_cache()
    ids_in_cache = {v["video_id"] for v in videos}
    removed = load_removed()
    try:
        data = fetch_list_by_channel()
    except QuotaExceeded as e:
        return added, errors + [str(e) + " (search.list)"]

    try:
        channel = fetch_channel_details()
    except QuotaExceeded as e:
        return added, errors + [str(e) + " (channels.list)"]

    pages = 0
    while True:
        pages += 1
        for item in data.get("items", []):
            if item["id"]["kind"] != "youtube#video":
                continue
            vid = item["id"]["videoId"]
            if vid in ids_in_cache or vid in removed:
                continue
            try:
                details, etag = fetch_video_details(vid)
                if not details:
                    continue
                sn = details["snippet"]; st = details.get("statistics", {})
                cd = details.get("contentDetails", {})
                duration = parse_iso8601_duration(cd.get("duration", ""))

                videos.append({
                    "video_id": vid,
                    "title": sn.get("title", ""),
                    "desc": sn.get("description", ""),
                    "tags": sn.get("tags", []),
                    "slug": slugify(sn.get("title", "")),
                    "creation_date": sn.get("publishedAt", "")[:10],
                    "last_edited_date": sn.get("publishedAt", "")[:10],
                    "view_count": int(st.get("viewCount", 0) or 0),
                    "like_count": int(st.get("likeCount", 0) or 0),  # some channels hide likes; keep safe
                    "comment_count": int(st.get("commentCount", 0) or 0),
                    "category": CATEGORIES.get(sn.get("categoryId", "22"), "People & Blogs"),
                    "duration_seconds": duration,
                    "channel_id": sn.get("channelId", ""),
                    "channel_title": channel["title"],
                    "subs": channel["subs"],
                    "etag": etag,
                    "fetched_at": utcnow().isoformat().replace("+00:00", "Z")
                })
                ids_in_cache.add(vid); added += 1
            except QuotaExceeded as e:
                errors.append(str(e) + f" while fetching video {vid}")
                save_cache(videos)
                return added, errors
            except Exception as e:
                errors.append(f"{vid}: {e}")

        if max_pages and pages >= max_pages:
            break
        next_page = data.get("nextPageToken")
        if not next_page:
            break
        if remaining_quota() < API_COSTS.get("search.list", 100):
            errors.append("Stopped early to conserve quota.")
            break
        try:
            data = fetch_list_by_channel(page_token=next_page)
        except QuotaExceeded as e:
            errors.append(str(e) + " during pagination")
            break

    save_cache(videos)
    return added, errors

# ---------- Minimal helpers & renderers ----------
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def _iso_with_tz(s: str | None) -> str:
    """
    Return an ISO-8601 datetime string with timezone (Z) for JSON-LD.
    - If given 'YYYY-MM-DD', make it 'YYYY-MM-DDT00:00:00Z'
    - If given ISO without tz, append 'Z'
    - If empty, use now (UTC)
    """
    if not s:
        return utcnow().isoformat().replace("+00:00", "Z")
    s = s.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":  # date only
        return f"{s}T00:00:00Z"
    try:
        # normalize anything ISO-like and force Z
        dtobj = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dtobj.isoformat().replace("+00:00", "Z")
    except Exception:
        # has time but likely missing tz; add Z
        if "T" in s and "Z" not in s and "+" not in s:
            return s + "Z"
        # last-resort: treat as date
        return f"{s[:10]}T00:00:00Z"

def iso_date(d: dt.date | None = None) -> str:
    return (d or utcnow().date()).isoformat()

def file_text(path: str | None) -> str | None:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None

def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def slugify(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s or ""

def output_basename(v: dict) -> str:
    """Choose the output filename base for a video page."""
    if SET.get("OUTPUT_NAMING", "slug").lower() == "id":
        return v.get("video_id") or v.get("slug") or slugify(v.get("title","")) or "video"
    return v.get("slug") or slugify(v.get("title","")) or (v.get("video_id") or "video")

def _int(n) -> int:
    try:
        return int(n)
    except Exception:
        return 0

def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "0"

def _short_desc(desc: str) -> str:
    if not desc:
        return ""
    first = next((ln.strip() for ln in desc.splitlines() if ln.strip()), desc.strip())
    return (first[:180] + "…") if len(first) > 180 else first

def _pretty_date(s: str | None) -> str | None:
    if not s:
        return None
    try:
        if len(s) == 10:
            dtobj = dt.datetime.strptime(s, "%Y-%m-%d")
        else:
            dtobj = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dtobj.strftime("%B %d, %Y")
    except Exception:
        return s

def _human_duration(sec:int|None) -> str:
    s = int(sec or 0)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts)

def build_dynamic_faqs(v: dict) -> list[tuple[str, str]]:
    title = (v.get("title") or "").strip()
    video_id = v.get("video_id", "")
    short_desc = _short_desc(v.get("desc", "")) or f"A {v.get('category','video')} on Martocci Mayhem."
    upload = _pretty_date(v.get("creation_date") or v.get("upload_date"))
    duration = _human_duration(_int(v.get("duration_seconds")))
    cat = v.get("category") or "People & Blogs"
    views = _fmt_int(v.get("view_count", 0))
    likes = _fmt_int(v.get("like_count", 0))
    comments = _fmt_int(v.get("comment_count", 0))
    tags = [t for t in (v.get("tags") or []) if t]
    top_tags = ", ".join(tags[:7]) if tags else "Martocci Mayhem, YouTube"
    ch_title = v.get("channel_title") or "Martocci Mayhem"
    handle = (SET.get("CHANNEL_HANDLE") or "").lstrip("@")
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    channel_url = f"https://www.youtube.com/@{handle}" if handle else ""
    is_asmr = ("asmr" in (title.lower() + " " + (v.get("desc","").lower())) or any("asmr" in t.lower() for t in tags))

    faqs: list[tuple[str,str]] = [
        (f"What is “{title}” about?", f"{short_desc}"),
        (f"Who created “{title}”?", f"{ch_title} (<a class=\"link\" href=\"{channel_url}\" target=\"_blank\" rel=\"noopener\">@{handle}</a>)" if handle else ch_title),
        (f"Where can I watch “{title}”?", f"You can watch it above or on YouTube: <a class=\"link\" href=\"{watch_url}\" target=\"_blank\" rel=\"noopener\">{watch_url}</a>."),
        (f"How long is “{title}”?", f"{duration}."),
        (f"When was “{title}” uploaded?", f"{upload or 'Upload date unavailable'}."),
        (f"What type of video is this?", f"{cat}."),
        (f"How popular is “{title}”?", f"Views: {views} · Likes: {likes} · Comments: {comments} (counts update on YouTube)."),
        (f"What are the key tags/keywords for this video?", top_tags),
        (f"Can I share or embed “{title}”?", "Yes. Use YouTube’s Share button for a link or the Embed option for an iframe. Please credit the channel."),
        (f"Any tips for the best experience?", "Headphones recommended for immersive audio and ASMR tingles." if is_asmr else "Turn sound on and watch in full-screen for the best experience.")
    ]
    return [(q, a) for q,a in faqs if (q and a)][:10]

def _linkify(text: str) -> str:
    if not text:
        return ""
    def repl(m):
        url = m.group(0)
        return f'<a class="link" href="{url}" target="_blank" rel="noopener">{url}</a>'
    text = re.sub(r"https?://[^\s<>\"']+", repl, text)
    return text.replace("\r\n", "\n").replace("\r", "\n")

_BOX = set("─│┼┌┐└┘═║╔╗╚╝╠╣╦╩█▌▐▄▀╞╪╡╟╢")

def _format_desc_for_web(text: str) -> str:
    """
    Escape HTML, linkify URLs (outside code), and wrap fenced/ASCII art runs in <pre class='ascii'>.
    Supports ```...``` and ~~~...~~~ fences.
    """
    if not text:
        return ""
    raw = _norm_newlines(text)

    # --- split into fenced code (``` or ~~~) and normal text segments (operate on RAW before escaping)
    fence_starts = ("```", "~~~")
    segments: list[tuple[str, str]] = []  # ("code" | "text", content)
    mode = "text"
    buf: list[str] = []
    fence_char = None

    def flush():
        if buf:
            segments.append((mode, "\n".join(buf)))
            buf.clear()

    for line in raw.split("\n"):
        ls = line.strip()
        if any(ls.startswith(fs) for fs in fence_starts):
            fst = next(fs for fs in fence_starts if ls.startswith(fs))
            if mode == "code" and fence_char == fst:
                # closing fence
                flush()
                mode, fence_char = "text", None
            elif mode == "text":
                flush()
                mode, fence_char = "code", fst
            # do not include the fence line itself
            continue
        buf.append(line)
    flush()

    # --- helpers
    STRUCT = set("-_|+=*/\\<>[](){}#^~")  # no . or : so plain sentences/URLs aren't "ascii-heavy"
    import re as _re

    # linkify only safe/plain text
    _url_re = _re.compile(r"https?://[^\s<>\"']+")
    def _linkify_safe(s: str) -> str:
        def repl(m):
            u = m.group(0)
            return f'<a class="link" href="{u}" target="_blank" rel="noopener">{u}</a>'
        return _url_re.sub(repl, s)

    def is_ascii_heavy(line: str) -> bool:
        s = line.rstrip("\n")
        if not s.strip():
            return False
        # treat indented blocks as code
        if _re.match(r"^(?: {4,}|\t)", s):
            return True
        # let URL-heavy lines be NORMAL text so they can be linkified & wrap
        if "http://" in s or "https://" in s:
            return False
        # any real box-drawing chars → ascii block
        if any(ch in _BOX for ch in s):
            return True
        # long runs of structural chars → ascii block
        if _re.search(r"([\-_=~*+#/\\|<>]{3,})", s):
            return True
        return False

    out_parts: list[str] = []

    for kind, content in segments:
        if kind == "code":
            # fence content: escape, no linkify
            out_parts.append(f'<pre class="ascii">{_html.escape(content)}</pre>')
            continue

        # non-fenced text: scan for ascii-heavy runs line-by-line
        lines = content.split("\n")
        i = 0
        while i < len(lines):
            if is_ascii_heavy(lines[i]):
                j = i
                while j < len(lines) and is_ascii_heavy(lines[j]):
                    j += 1
                block = "\n".join(lines[i:j])
                out_parts.append(f'<pre class="ascii">{_html.escape(block)}</pre>')
                i = j
            else:
                # collect a stretch of plain text, escape + linkify, keep line breaks
                j = i
                plain: list[str] = []
                while j < len(lines) and not is_ascii_heavy(lines[j]):
                    plain.append(lines[j]); j += 1
                escaped = _html.escape("\n".join(plain))
                out_parts.append(_linkify_safe(escaped))
                i = j

    return "\n".join(out_parts)

def _norm_newlines(s: str) -> str:
    return (s or "").replace("\r\n", "\n").replace("\r", "\n")

def build_related_list(all_videos: list[dict], current: dict, k: int = 10) -> list[dict]:
    """Very simple related scorer using shared tags + category + recency/popularity."""
    cur_id = current.get("video_id")
    cur_tags = set((current.get("tags") or []))
    cur_cat  = current.get("category")
    def score(v):
        if v.get("video_id") == cur_id:
            return -1e9
        s = 0.0
        vt = set((v.get("tags") or []))
        s += 3 * len(vt & cur_tags)
        if cur_cat and v.get("category") == cur_cat:
            s += 2
        s += 0.1 if (v.get("last_edited_date") or v.get("creation_date")) else 0
        s += min(_int(v.get("view_count", 0)) / 1_000_000.0, 1.0)
        return s
    items = sorted(all_videos, key=score, reverse=True)
    out = []
    for v in items[:k]:
        out.append({
            "slug": output_basename(v),
            "video_id": v.get("video_id",""),
            "title": v.get("title",""),
            "desc": v.get("desc",""),
            "view_count": _fmt_int(v.get("view_count",0)),
            "last_edited_date": v.get("last_edited_date") or v.get("creation_date") or ""
        })
    return out

def notify(title: str, message: str):
    try:
        if plyer_notify and SET.get("NOTIFY_ON_COMPLETE", True):
            plyer_notify.notify(title=title, message=message, app_name=APP_NAME, timeout=5)
    except Exception:
        pass

def _get_env():
    if "jinja2" in [m[0] for m in _missing]:
        raise RuntimeError("jinja2 is not installed")
    loader = jinja2.ChoiceLoader([
        jinja2.DictLoader({
            "video_template.html": VIDEO_TEMPLATE,
            "index_template.html": INDEX_TEMPLATE,
        }),
        jinja2.FileSystemLoader(TEMPLATE_DIR),
    ])
    env = jinja2.Environment(
        loader=loader,
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # Ensure tojson exists
    try:
        from markupsafe import Markup
    except Exception:
        class Markup(str): pass
    import json as _json
    if "tojson" not in env.filters:
        env.filters["tojson"] = lambda x: Markup(_json.dumps(x, ensure_ascii=False))
    return env

def render_video_page(v: dict, out_dir: str) -> tuple[str, str]:
    """
    Render a single video page; returns (output_path, html).
    NOTE: This function DOES NOT write to disk; caller handles diffs/writes.
    """
    env = _get_env()
    tmpl = env.get_template("video_template.html")

    subs_val   = _int(v.get("subs") or v.get("subscriber_count") or 0)
    subs_known = (v.get("subs") is not None) or (v.get("subscriber_count") is not None)

    # ---- Gather core fields first (so they're available to all builders)
    ch_handle   = (SET.get("CHANNEL_HANDLE") or "").lstrip("@")
    title       = (v.get("title") or "").strip()
    slug        = output_basename(v)                 # <--- define BEFORE using anywhere
    video_id    = v.get("video_id", "")
    category    = v.get("category") or "People & Blogs"

    # prefer full datetime; otherwise fall back to date
    upload_raw     = v.get("upload_datetime") or v.get("creation_date") or v.get("upload_date") or ""
    last_edit_raw  = v.get("last_edited_date") or v.get("creation_date") or ""

    # ---- Page FAQs (visual) + JSON-LD (built in Python)
    faqs_list = v.get("faqs") or build_dynamic_faqs(v)
    faq_schema = None
    if faqs_list:
        faq_schema = {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": q,
                    "acceptedAnswer": {"@type": "Answer", "text": a}
                } for (q, a) in faqs_list
            ]
        }

    # ---- VideoObject JSON-LD (built in Python to omit empty fields)
    tags_list     = [t for t in (v.get("tags") or []) if t]
    keywords_text = ", ".join(map(str, tags_list)) if tags_list else None
    duration_sec  = _int(v.get("duration_seconds", 0))
    duration_iso  = f"PT{duration_sec}S" if duration_sec > 0 else None
    view_count_i  = _int(v.get("view_count", 0))

    video_schema = {
        "@context": "https://schema.org",
        "@type": "VideoObject",
        "@id": f"{SITE_URL}/videos/{slug}.html#video",
        "name": title,
        "description": _short_desc(v.get("desc", "")),
        "thumbnailUrl": [f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"],
        "image": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        "uploadDate": _iso_with_tz(upload_raw),                 # ensures Z
        "duration": f"PT{duration_sec}S" if duration_sec > 0 else None,
        "embedUrl": f"https://www.youtube.com/embed/{video_id}",
        "contentUrl": f"https://www.youtube.com/watch?v={video_id}",
        "url": f"{SITE_URL}/videos/{slug}.html",
        "publisher": {"@id": f"{SITE_URL}/#org"},
        "isFamilyFriendly": True,
        "inLanguage": "en",
        "genre": category,
        "interactionStatistic": {
            "@type": "InteractionCounter",
            "interactionType": {"@type": "WatchAction"},
            "userInteractionCount": _int(v.get("view_count", 0)),
        },
        "potentialAction": [
            {"@type": "WatchAction", "target": f"https://www.youtube.com/watch?v={video_id}"},
            {
                "@type": "SeekToAction",
                "target": {
                    "@type": "EntryPoint",
                    "urlTemplate": f"https://www.youtube.com/watch?v={video_id}&t={{seek_to_second_number}}"
                },
                "startOffset-input": "required name=seek_to_second_number"
            }
        ],
        "sameAs": [f"https://www.youtube.com/watch?v={video_id}"],
    }
    # Strip any Nones/empties so the JSON-LD stays clean
    video_schema = {k: v for k, v in video_schema.items() if v not in (None, [], {})}

    if duration_iso:
        video_schema["duration"] = duration_iso
    if keywords_text:
        video_schema["keywords"] = keywords_text

    # ---- Final Jinja context
    context = {
        "site_url": SITE_URL,
        "current_year": dt.date.today().year,

        "app_version": APP_VERSION,   # ← add this
        "build_stamp": BUILD_STAMP,   # ← add this

        "title": title,
        "short_desc": _short_desc(v.get("desc","")),
        "formatted_desc": _format_desc_for_web(v.get("desc","")),
        "video_id": video_id,
        "slug": slug,
        "category": category,  # if you reference it in the template
        "tags": tags_list,     # available for any future template use

        "upload_date": _iso_with_tz(upload_raw),
        "upload_date_pretty": _pretty_date(upload_raw),
        "last_edited_date_pretty": _pretty_date(last_edit_raw),

        "view_count": _fmt_int(view_count_i),
        "view_count_raw": view_count_i,
        "like_count": _fmt_int(v.get("like_count",0)),
        "comment_count": _fmt_int(v.get("comment_count",0)),
        "duration_seconds": duration_sec,

        "channel_title": v.get("channel_title",""),
        "channel_handle": ch_handle,
        "subscriber_count": _fmt_int(subs_val),
        "subs_known": subs_known,
        "subs_raw": bool(_int(v.get("subs") or v.get("subscriber_count") or 0)),

        "related": v.get("related") or [],
        "faqs": faqs_list,           # visual accordion
        "faq_schema": faq_schema,    # JSON-LD (only rendered if present)
        "video_schema": video_schema # JSON-LD (always rendered)
    }

    html = tmpl.render(**context)
    out_path = os.path.join(out_dir, f"{slug}.html")
    return out_path, html

def render_index(videos: list[dict], out_path: str) -> tuple[str, str]:
    """
    Render the homepage index; returns (output_path, html).
    NOTE: This function DOES NOT write to disk; caller handles diffs/writes.
    """
    env = _get_env()
    tmpl = env.get_template("index_template.html")

    data = []
    for v in videos:
        data.append({
            "slug": output_basename(v),
            "video_id": v.get("video_id",""),
            "title": v.get("title",""),
            "desc": v.get("desc",""),
            "category": v.get("category","People & Blogs"),
            "tags": v.get("tags",[]) or [],
            "view_count": _int(v.get("view_count",0)),
            "like_count": _int(v.get("like_count",0)),
            "comment_count": _int(v.get("comment_count",0)),
            "last_edited_date": v.get("last_edited_date") or "",
            "creation_date": v.get("creation_date") or "",
        })

    web_page_schema = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": "Martocci Mayhem",
        "url": SITE_URL,
        "description": "Explore ASMR, comedy, shorts, and tech-heavy experiments.",
    }
    item_list_schema = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "itemListElement": [
            {"@type": "ListItem", "position": i+1,
             "url": f"{SITE_URL}/videos/{d['slug']}.html",
             "name": d.get("title") or d.get("slug")}
            for i, d in enumerate(data[:200])
        ]
    }

    html = tmpl.render(
        site_url=SITE_URL,
        current_year=dt.date.today().year,
        app_version=APP_VERSION,      # ← add this
        build_stamp=BUILD_STAMP,      # ← add this
        videos=data,
        web_page_schema=web_page_schema,
        item_list_schema=item_list_schema,
    )

    return out_path, html

# ---------- Update Site (with progress callback) ----------
def update_site(update_index=True, update_videos=True, dry_run=False, commit_msg=None, progress=None, force_rebuild=False, deploy_ui_progress=None):

    def _prog(pct: float, msg: str):
        if progress:
            try: progress(max(0.0, min(1.0, float(pct))), msg)
            except Exception: pass

    _prog(0.02, "Preparing")
    ensure_templates(force=True)

    videos_all = load_cache()
    removed_ids = load_removed()
    videos = [v for v in videos_all if v.get("video_id") not in removed_ids and not v.get("deleted")]

    changed_files, diffs, out_urls = [], [], []

    def prune_orphan_pages(removed_ids:set[str], videos_all:list[dict]) -> list[str]:
        """Delete /videos/*.html for anything removed."""
        existing = set(os.listdir(VIDEOS_DIR)) if os.path.exists(VIDEOS_DIR) else set()
        id_to_slug = {v.get("video_id"): output_basename(v) for v in videos_all}
        removed_slugs = { (id_to_slug.get(i) or i).lower() for i in removed_ids }
        deleted = []
        for base in removed_slugs:
            fname = f"{base}.html"
            p = os.path.join(VIDEOS_DIR, fname)
            if fname in existing:
                try:
                    os.remove(p); deleted.append(p)
                except Exception:
                    pass
        return deleted
    
    def _prune_changed_slugs(videos_now: list[dict]) -> list[str]:
        keep = {f"{output_basename(v)}.html" for v in videos_now}
        deleted = []
        if os.path.isdir(VIDEOS_DIR):
            for fname in os.listdir(VIDEOS_DIR):
                if fname.endswith(".html") and fname not in keep:
                    try:
                        os.remove(os.path.join(VIDEOS_DIR, fname)); deleted.append(fname)
                    except Exception: pass
        return [os.path.join(VIDEOS_DIR, f) for f in deleted]

    _prog(0.06, "Building recommendations")
    try:
        for v in videos:
            v["related"] = build_related_list(videos, v, k=10)
    except Exception as e:
        log.warning("Related builder failed: %s", e)

    if update_videos and videos:
        n = len(videos)
        for i, v in enumerate(videos, 1):
            out_dir = PREVIEW_DIR if dry_run else VIDEOS_DIR
            out_path, html = render_video_page(v, out_dir)

            # force touch without changing template logic
            if force_rebuild:
                html += f"\n<!-- rebuild:{utcnow().isoformat()} -->"

            # When dry-run, compare against the REAL file in /videos (not _preview)
            cmp_path  = os.path.join(VIDEOS_DIR, os.path.basename(out_path))
            baseline  = file_text(cmp_path) if dry_run else file_text(out_path if os.path.exists(out_path) else None)

            write_text(out_path, html)

            # record changes + diff and URL to ping
            if (baseline or "") != (html or ""):
                changed_files.append(cmp_path if dry_run else out_path)
                ud = difflib.unified_diff(
                    (baseline or "").splitlines(keepends=True),
                    (html or "").splitlines(keepends=True),
                    fromfile=f"{cmp_path}:old",
                    tofile=f"{cmp_path}:new",
                    lineterm=""
                )
                diffs.append("\n".join(ud))
                out_urls.append(f"{SITE_URL}/videos/{output_basename(v)}.html")

            _prog(0.08 + 0.60 * (i / max(1, n)), f"Rendering video {i}/{n}")

    if not dry_run:
        deleted_files = prune_orphan_pages(removed_ids, videos_all)
        deleted_files += _prune_changed_slugs(videos)
        if deleted_files:
            changed_files.extend(deleted_files)
    else:
        # Optional: show what would be deleted without touching disk
        would_delete = []
        keep = {f"{output_basename(v)}.html" for v in videos}
        if os.path.isdir(VIDEOS_DIR):
            for fname in os.listdir(VIDEOS_DIR):
                if fname.endswith(".html") and fname not in keep:
                    would_delete.append(os.path.join(VIDEOS_DIR, fname))
        changed_files.extend([f"[would delete] {p}" for p in would_delete])

    if update_index:
        _prog(0.72, "Rendering index")
        idx_path = os.path.join(PREVIEW_DIR if dry_run else REPO_PATH, "index.html")
        out_path, html = render_index(videos, idx_path)
        # Compare against the real index when dry-run
        baseline = file_text(os.path.join(REPO_PATH, "index.html")) if dry_run else file_text(out_path if os.path.exists(out_path) else None)
        write_text(out_path, html)
        if (baseline or "") != (html or ""):
            changed_files.append(idx_path if dry_run else os.path.join(REPO_PATH, "index.html"))
            ud = difflib.unified_diff(
                (baseline or "").splitlines(keepends=True),
                html.splitlines(keepends=True),
                fromfile="index.html:old", tofile="index.html:new", lineterm=""
            )
            diffs.append("\n".join(ud))
            out_urls.append(SITE_URL)

    _prog(0.86, "Regenerating sitemap")
    sm_path_real, sm_xml = regenerate_sitemap(videos)
    sm_path_write = os.path.join(PREVIEW_DIR, "sitemap.xml") if dry_run else sm_path_real
    baseline = file_text(sm_path_real if dry_run else sm_path_write)
    write_text(sm_path_write, sm_xml)
    if (baseline or "") != (sm_xml or ""):
        changed_files.append(sm_path_write if dry_run else sm_path_real)

    deploy_output = ""
    if not dry_run:
        _prog(0.92, "Deploying")
        commit_msg = commit_msg or SET["DEFAULT_COMMIT_MSG"]

        # show the bottom deploy bar (if present in the GUI)
        try:
            if dpg and dpg.does_item_exist("deploy_progress_wrap"):
                dpg.configure_item("deploy_progress_wrap", show=True)
                dpg.set_value("deploy_progress_bar", 0.0)
                dpg.configure_item("deploy_progress_bar", overlay="0%")
                dpg.set_value("deploy_progress_text", "Starting deploy…")
        except Exception:
            pass

        def _dprog(frac, msg):
            # Map 0..1 inside deploy() to 92..100% on the outer bar
            f = max(0.0, min(1.0, float(frac)))
            _prog(0.92 + 0.08 * f, f"Deploying: {msg}")
            # Drive the inner (bottom) deploy bar directly
            try:
                if deploy_ui_progress:
                    deploy_ui_progress(f, str(msg))
            except Exception:
                pass

        # Call deploy ONCE using the callback above
        try:
            deploy_output = deploy(commit_msg, out_urls, prog=_dprog)
        except Exception as e:
            deploy_output = f"Deploy failed: {e}"

        # Save first diff for inspection
        try:
            if diffs:
                os.makedirs(os.path.join(REPO_PATH, "_preview"), exist_ok=True)
                write_text(os.path.join(REPO_PATH, "_preview", "__last_first_diff.txt"), diffs[0])
        except Exception:
            pass

    try:
        if deploy_ui_progress:
            deploy_ui_progress(1.0, "done")
    except Exception:
        pass

    _prog(1.0, "Done")

    return {"changed_files": changed_files, "diffs": diffs, "deploy_output": deploy_output, "dry_run": dry_run, "urls": out_urls}

# ---------- CLI ----------
def run_cli(args):
    ensure_templates()
    if getattr(args, "reslug_title_oembed", False):
        changes = reslug_cache_from_oembed(only_if_id_like=True)
        print(f"Reslugged via oEmbed: {len(changes)}")
        if changes[:10]:
            print("Examples:")
            for vid, old, new in changes[:10]:
                print(f"  {vid}: {old}  ->  {new}")

    if args.reslug_title:
        changes = reslug_cache_from_title(only_if_id_like=True)
        print(f"Reslugged {len(changes)} videos (from ID-like slugs to title-based slugs).")
        if changes:
            print("Examples:")
            for vid, old, new in changes[:10]:
                print(f"  {vid}: {old or '(none)'}  ->  {new}")

    if args.fetch_latest is not None:
        added, errors = fetch_latest(days=args.fetch_latest)
        print(f"Fetched latest (<= {args.fetch_latest} days): added {added}")
        if errors:
            print("Errors:\n - " + "\n - ".join(errors))

    if args.fetch_all:
        added, errors = fetch_all()
        print(f"Fetched ALL: added {added}")
        if errors:
            print("Errors:\n - " + "\n - ".join(errors))

    if args.refresh_stale is not None:
        updated, errors = refresh_stale(days=args.refresh_stale)
        print(f"Smart refresh stale >={args.refresh_stale} days: updated {updated}")
        if errors:
            print("Errors:\n - " + "\n - ".join(errors))

    if args.update_index or args.update_videos:
        res = update_site(
            update_index=args.update_index,
            update_videos=args.update_videos,
            dry_run=args.dry_run,
            commit_msg=args.commit_msg,
            force_rebuild=getattr(args, "force_rebuild", False),  # <-- fix
        )

        print(f"Changed files: {len(res['changed_files'])}")
        if res["dry_run"]:
            print("Dry-run diffs (first shown):")
            if res["diffs"]:
                print(res["diffs"][0][:2000])
        else:
            print(res["deploy_output"].strip())

# --- GUI helpers (missing in last build) -------------------------------------
def big_button(label, callback=None, *, width=0, height=36, tag=None, **kwargs):

    def _as_int(v, default):
        if isinstance(v, bool):
            return v
        try:
            return int(v)
        except Exception:
            return int(default)

    width  = _as_int(width, -1)
    height = _as_int(height, 36)

    if "indent" in kwargs and kwargs["indent"] is not None:
        kwargs["indent"] = _as_int(kwargs["indent"], 0)
    if "pos" in kwargs and kwargs["pos"]:
        x, y = kwargs["pos"]
        kwargs["pos"] = (_as_int(x, 0), _as_int(y, 0))
    if "tracked" in kwargs and kwargs["tracked"] is not None:
        kwargs["tracked"] = bool(kwargs["tracked"])
    if "track_offset" in kwargs and kwargs["track_offset"] is not None:
        try:
            kwargs["track_offset"] = float(kwargs["track_offset"])
        except Exception:
            kwargs["track_offset"] = 0.0
    if "direction" in kwargs and kwargs["direction"] is not None:
        kwargs["direction"] = _as_int(kwargs["direction"], 0)

    # build args dict so we can omit width when using auto size
    args = dict(label=label, callback=callback, height=height, **kwargs)
    if width not in (None, 0):
        args["width"] = width
    if tag is not None:
        args["tag"] = tag


    btn = dpg.add_button(**args)

    try:
        dpg.bind_item_theme(btn, "theme_btn_primary")
    except Exception:
        pass
    return btn

def _run_thread(fn, *args, **kwargs):
    import threading as _th
    def _wrap():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            _append_log("be_log_text", f"Worker error: {e}")
    t = _th.Thread(target=_wrap, daemon=True)
    t.start()
    return t

def _append_log(tag_or_msg, msg: str | None = None):
    tag = "be_log_text" if msg is None else tag_or_msg
    message = tag_or_msg if msg is None else msg
    try:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        for t in (tag, "logs_text"):
            if dpg and dpg.does_item_exist(t):
                prev = dpg.get_value(t) or ""
                dpg.set_value(t, prev + f"[{ts}] {message}\n")
    except Exception:
        pass

def _set_text(tag, text: str):
    """Safe set text for any item if present."""
    try:
        if dpg.does_item_exist(tag):
            dpg.set_value(tag, text)
    except Exception:
        pass

def _reload_quota_text():
    """Refresh the small quota line in the header, if present."""
    try:
        load_quota()  # will roll the day only at PT midnight
    except Exception:
        pass
    try:
        if PT_TZ:
            now_pt = dt.datetime.now(PT_TZ)
            tomorrow_pt = (now_pt + dt.timedelta(days=1)).date()
            reset_at = dt.datetime.combine(tomorrow_pt, dt.time(0, 0), tzinfo=PT_TZ)
        else:
            # fallback approximation: UTC-8
            now_utc = dt.datetime.now(dt.timezone.utc)
            now_pt = now_utc - dt.timedelta(hours=8)
            reset_at = (now_pt + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            secs = max(0, int((reset_at - now_pt).total_seconds()))
            h, r = divmod(secs, 3600)
            m, _ = divmod(r, 60)

        _set_text(
            "quota_text_gui",
            f"Quota (est, PT): used {estimated_units:,} / {DAILY_QUOTA_LIMIT:,} "
            f"(remaining {remaining_quota():,}) · resets in {h}h {m}m"
        )
    except Exception:
        pass

def _is_quota_exceeded(err) -> bool:
    """Best-effort detection for YouTube API quota errors from googleapiclient.HttpError."""
    try:
        code = getattr(getattr(err, "resp", None), "status", None)
    except Exception:
        code = None
    s = str(err).lower()
    return (code in (403, 429)) or ("quota" in s and "exceed" in s) or ("rate" in s and "limit" in s)

def _quota_trip_ui(where: str = ""):
    _append_log("be_log_text", f"Quota exceeded during {where}. Falling back to cache when possible.")
    try: mark_quota_exhausted()
    except QuotaExceeded:
        mark_quota_exhausted()
        _append_log("YouTube quota exceeded during refresh; using cache where possible.")
    except Exception: _reload_quota_text()

# ===================== MINIMAL ROOT UI (scaffold) =====================
def _build_minimal_root_ui():
    # Make sure we can safely (re)create the root window without alias conflicts
    if dpg.does_item_exist("root"):
        try:
            dpg.delete_item("root")  # delete the actual window, not just children
        except Exception:
            pass

    with dpg.window(tag="root",
                    no_title_bar=True, no_move=True, no_resize=True, no_collapse=True,
                    no_scrollbar=True):
        ...

def _status_tick(sender=None, app_data=None, user_data=None):
    """Runs on the main thread. Safe to call DPG here."""
    try:
        _update_status_badges()
    except Exception:
        pass
    # reschedule next tick ~5s later (300 frames at ~60fps)
    try:
        next_frame = dpg.get_frame_count() + 300
        dpg.set_frame_callback(next_frame, _status_tick)
    except Exception:
        pass

def _start_status_timer():
    """Kick off the recurring main-thread status updater."""
    try:
        # first run ≈ 1s after startup (60 frames)
        first = dpg.get_frame_count() + 60
        dpg.set_frame_callback(first, _status_tick)
    except Exception:
        pass

# ===================== GUI ENTRY =====================
def start_gui():
    if dpg is None:
        raise RuntimeError("Dear PyGui is not installed. Run: pip install dearpygui")

    # 1) Context + viewport
    dpg.create_context()
    try:
        dpg.create_viewport(title=f"{APP_NAME} v{APP_VERSION}", width=1400, height=900)
    except Exception:
        pass

    # 2) Build all UI (this creates the 'root' window exactly once)
    _refresh_header()

    # 3) Setup + show
    try:
        dpg.setup_dearpygui()
    except Exception:
        pass
    try:
        dpg.show_viewport()
    except Exception:
        pass
    try:
        dpg.set_primary_window("root", True)
    except Exception:
        pass

    # 4) Fonts (safe to bind again)
    try:
        fonts = prepare_fonts()
        if isinstance(fonts, dict) and fonts.get("ui"):
            dpg.bind_font(fonts["ui"])
    except Exception:
        pass

    # 5) Maximize (best-effort)
    try:
        dpg.maximize_viewport()
    except Exception:
        pass

    # 6) Run loop
    dpg.start_dearpygui()
    dpg.destroy_context()

def _bind_tab_text_white_to_all_tab_bars():
    """Only tab labels are white; nothing else is affected."""
    if dpg.does_item_exist("__theme_tabtext_white__"):
        dpg.delete_item("__theme_tabtext_white__")
    with dpg.theme(tag="__theme_tabtext_white__"):
        with dpg.theme_component(dpg.mvTab):
            dpg.add_theme_color(dpg.mvThemeCol_Text, (255, 255, 255, 255))

    for item in dpg.get_all_items():
        info = dpg.get_item_info(item)
        if info and info.get("type") == "mvAppItemType::mvTabBar":
            dpg.bind_item_theme(item, "__theme_tabtext_white__")

# ===================== HEADER STATUS HELPERS =====================

def _status_color(level: str):
    # "ok"=green, "warn"=orange, "error"=red, "muted"=gray
    return {
        "ok":    (34, 197, 94, 255),    # emerald-500
        "warn":  (245, 158, 11, 255),   # amber-500
        "error": (239, 68, 68, 255),    # red-500
        "muted": (160, 160, 160, 255),
    }.get(level, (160,160,160,255))

def _youtube_connectivity():
    """
    Returns (level, text, color).
    level: "ok" | "warn" | "error"
    """
    # If Google libs missing, show warning (user can still see why)
    if any("google" in n for (n, _) in _missing):
        return ("warn", "YouTube: libraries missing (install auth/client)", _status_color("warn"))

    token_file = "token.json"
    if not os.path.exists(token_file):
        return ("error", "YouTube: Not connected", _status_color("error"))

    # Try to inspect token expiry without requiring google-auth
    try:
        with open(token_file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                data = {}
        expiry = data.get("expiry")
        if expiry:
            try:
                # Handle 'Z' by replacing with +00:00
                exp = dt.datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                now = dt.datetime.now(dt.timezone.utc)
                if exp > now:
                    # Prefer channel handle/title if we know it
                    label = f"YouTube: Connected — {CHANNEL_HANDLE or data.get('client_id','')}"
                    return ("ok", label.strip(), _status_color("ok"))
                else:
                    return ("warn", "YouTube: Token expired (re-auth needed)", _status_color("warn"))
            except Exception:
                return ("warn", "YouTube: Token present (unknown expiry)", _status_color("warn"))
        else:
            return ("warn", "YouTube: Token present (re-auth to verify)", _status_color("warn"))
    except Exception:
        return ("warn", "YouTube: Token unreadable", _status_color("warn"))

def _git_summary():
    """
    Returns (branch, changes, last_commit_str) or (None, None, None)
    """
    if any(n == "gitpython" for (n, _) in _missing):
        return (None, None, None)
    try:
        repo = Repo(REPO_PATH)
        try:
            branch = getattr(repo.active_branch, "name", "(detached)")
        except Exception:
            branch = "(detached)"
        # count porcelain status lines
        try:
            lines = repo.git.status("--porcelain").strip().splitlines()
            changes = len([ln for ln in lines if ln.strip()])
        except Exception:
            changes = None
        try:
            lc = repo.head.commit.committed_datetime
            last_commit_str = lc.strftime("%Y-%m-%d %H:%M")
        except Exception:
            last_commit_str = None
        return (branch, changes, last_commit_str)
    except Exception:
        return (None, None, None)

def _site_ping():
    """
    Returns (level, text, color) about SITE_URL reachability (fast, 1s timeout).
    """
    url = (SITE_URL or "").strip()
    if not url.startswith("http"):
        return ("muted", "Site: —", _status_color("muted"))
    try:
        if any(n == "requests" for (n, _) in _missing):
            return ("muted", "Site: (requests not installed)", _status_color("muted"))
        r = requests.head(url, timeout=1, allow_redirects=True)
        if 200 <= r.status_code < 400:
            return ("ok", f"Site: Online ({r.status_code})", _status_color("ok"))
        elif 400 <= r.status_code < 500:
            return ("warn", f"Site: {r.status_code}", _status_color("warn"))
        else:
            return ("error", f"Site: {r.status_code}", _status_color("error"))
    except Exception:
        return ("error", "Site: unreachable", _status_color("error"))

def _update_status_badges():
    # YouTube
    lvl, txt, col = _youtube_connectivity()
    if dpg.does_item_exist("hdr_conn_dot"):
        try: dpg.set_value("hdr_conn_dot", col)
        except Exception: dpg.configure_item("hdr_conn_dot", default_value=col)
    if dpg.does_item_exist("hdr_conn_text"):
        dpg.set_value("hdr_conn_text", txt)

    # Site
    sl, stxt, scol = _site_ping()
    if dpg.does_item_exist("hdr_site_dot"):
        try: dpg.set_value("hdr_site_dot", scol)
        except Exception: dpg.configure_item("hdr_site_dot", default_value=scol)
    if dpg.does_item_exist("hdr_site_text"):
        dpg.set_value("hdr_site_text", stxt)

    # Git
    br, chg, last = _git_summary()
    _set_text("hdr_git_branch",  f"{br or '—'}")
    _set_text("hdr_git_changes", f"{'—' if chg is None else chg} changes")
    _set_text("hdr_git_last",    f"Last commit {last or '—'}")

def _inject_status_row():
    """Create the modern status row once, under the left header column."""
    if not dpg.does_item_exist("hdr_left") or dpg.does_item_exist("hdr_status_row"):
        return
    with dpg.group(tag="hdr_left"):
    # Row 1: Channel
        with dpg.group(horizontal=True):
            dpg.add_text("Channel:", color=(120,120,120,255))
            dpg.add_text(CHANNEL_HANDLE or "—", tag="hdr_channel")

        dpg.add_spacer(height=4)

        # Row 2: Quota
        with dpg.group(horizontal=True):
            dpg.add_text("Quota:", color=(120,120,120,255))
            dpg.add_text("", tag="quota_text_gui")

        # --- Status row: always render, even offline ---
        if not dpg.does_item_exist("hdr_status_row"):
            with dpg.group(horizontal=True, tag="hdr_status_row"):
                dpg.add_text("Status", color=(120,120,120,255))

            # YouTube
            dpg.add_color_button(tag="hdr_conn_dot", default_value=(160,160,160,255),
                                no_alpha=True, width=12, height=12)
            dpg.add_text("YouTube: Not connected", tag="hdr_conn_text")

            dpg.add_spacer(width=12)

            # Site
            dpg.add_color_button(tag="hdr_site_dot", default_value=(160,160,160,255),
                                no_alpha=True, width=12, height=12)
            dpg.add_text("Site: not checked", tag="hdr_site_text")

            dpg.add_spacer(width=12)

            # Git
            dpg.add_text("Git:", color=(180,180,180,255))
            dpg.add_text("—", tag="hdr_git_branch")
            dpg.add_text("•", color=(120,120,120,255))
            dpg.add_text("— changes", tag="hdr_git_changes")
            dpg.add_text("•", color=(120,120,120,255))
            dpg.add_text("Last commit —", tag="hdr_git_last")

def _refresh_header():
    """Updates quota bar + labels safely."""
    try:
        _reload_quota_text()  # updates 'quota_text_gui'
    except Exception:
        pass

    # Drive the quota progress bar from the text line
    used, total = 0, 0
    try:
        import re as _re
        t = dpg.get_value("quota_text_gui") or ""
        m = _re.search(r'(\d[\d,]*)\s*/\s*(\d[\d,]*)', t)
        if m:
            used  = int(m.group(1).replace(",", ""))
            total = int(m.group(2).replace(",", ""))
    except Exception:
        pass

    try:
        frac = (used / total) if total else 0.0
        frac = max(0.0, min(1.0, frac))
        if dpg.does_item_exist("hdr_quota_bar"):
            dpg.set_value("hdr_quota_bar", frac)
            dpg.configure_item("hdr_quota_bar", overlay=f"{used:,} / {total:,}")
    except Exception:
        pass

    # Videos in cache
    try:
        vids = load_cache() or []
        if dpg.does_item_exist("hdr_videos"):
            dpg.set_value("hdr_videos", f"Videos in cache: {len(vids):,}")
    except Exception:
        if dpg.does_item_exist("hdr_videos"):
            dpg.set_value("hdr_videos", "Videos in cache: —")

    fonts = prepare_fonts()
    if fonts.get("ui"):
        dpg.bind_font(fonts["ui"])

    def _run_site_update_from_ui():
        # read your UI options
        try:
            dry = bool(dpg.get_value("opt_dry_run")) if dpg.does_item_exist("opt_dry_run") else False
        except Exception:
            dry = False
        try:
            force = bool(dpg.get_value("opt_force_rebuild")) if dpg.does_item_exist("opt_force_rebuild") else False
        except Exception:
            force = False
        try:
            msg = dpg.get_value("commit_msg") if dpg.does_item_exist("commit_msg") else None
        except Exception:
            msg = None

        try:
            dpg.configure_item("deploy_progress_wrap", show=False)
            dpg.set_value("deploy_progress_bar", 0.0)
            dpg.configure_item("deploy_progress_bar", overlay="0%")
            dpg.set_value("deploy_progress_text", "")
        except Exception:
            pass

        # reset the progress UI immediately
        try:
            dpg.set_value("be_progress_bar", 0.0)
            dpg.configure_item("be_progress_bar", overlay="0%")
            dpg.set_value("be_progress_text", "Preparing…")
        except Exception:
            pass

        def _worker():
            try:
                update_site(
                    update_index=True,
                    update_videos=True,
                    dry_run=dry,
                    commit_msg=msg,
                    progress=be_progress,           # outer/top bar
                    force_rebuild=force,            # reads your checkbox
                    deploy_ui_progress=deploy_progress  # inner/bottom bar
                )

            except Exception as e:
                _append_log("logs_text", f"Update failed: {e}")
                try:
                    be_progress(1.0, "Failed — see Logs")
                except Exception:
                    pass

        _run_thread(_worker)

    # Replace your be_progress() with this version
    def be_progress(pct: float, msg: str | None = None):
        """Monotonic progress for both tabs; drives the TOP bar(s)."""
        try:
            p = max(0.0, min(1.0, float(pct)))
        except Exception:
            p = 0.0
        # monotonic
        if getattr(be_progress, "_last", 0.0) > p:
            p = be_progress._last
        be_progress._last = p

        # Bulk Editor top bar
        try:
            dpg.set_value("be_progress", p)
            dpg.configure_item("be_progress", overlay=f"{int(p*100)}%")
        except Exception:
            pass

        # Update tab main bar
        try:
            dpg.set_value("progress", p)
            dpg.configure_item("progress", overlay=f"{int(p*100)}%")
        except Exception:
            pass

        # (Legacy fallback, safe to keep if still present)
        try:
            dpg.set_value("be_progress_bar", p)
            dpg.configure_item("be_progress_bar", overlay=f"{int(p*100)}%")
        except Exception:
            pass

        # Status line under the BE bar
        try:
            dpg.set_value("be_progress_text", str(msg or ""))
        except Exception:
            pass

    def deploy_progress(pct: float, msg: str):
        """Bottom (inner) progress bar shown only during deploy.
        Also mirrors to the top bar so you see movement."""
        try:
            pct = max(0.0, min(1.0, float(pct)))
        except Exception:
            pct = 0.0

        # Bottom bar (deploy-only)
        try: dpg.set_value("deploy_progress_bar", pct)
        except Exception: pass
        try: dpg.configure_item("deploy_progress_bar", overlay=f"{int(pct*100)}%")
        except Exception: pass
        try: dpg.set_value("deploy_progress_text", str(msg))
        except Exception: pass

        # Mirror to the TOP bar (maps 0..1 inside deploy to 92..100% overall)
        try:
            outer = 0.92 + 0.08 * pct
            dpg.set_value("be_progress", outer)
            dpg.configure_item("be_progress", overlay=f"{int(outer*100)}%")
            dpg.set_value("be_progress_text", f"Deploying: {msg}")
        except Exception:
            pass

    # Paint real values (works offline too)
    try:
        if dpg is not None:
            _update_status_badges()
    except Exception:
        pass

    # ===================== THEMES + TAB TEXT + MAXIMIZE =====================

    def build_modern_themes(accent=(56, 126, 245, 255)):
        """
        Builds two base themes (dark/light) with proper colors.
        - Only TAB LABELS will be white (via a small, separate theme bound to tab bars).
        - All other text in LIGHT mode is dark.
        - Buttons stay blue in both modes.
        """

        def make(tag, mode="dark"):
            if mode == "dark":
                BG        = (15, 15, 15, 255)
                BG_CARD   = (17, 17, 17, 255)
                TEXT      = (235, 235, 235, 255)
                MUTED     = (160, 160, 160, 255)
                FRAME     = (28, 28, 28, 255)
                FRAME_H   = (38, 38, 38, 255)
                FRAME_A   = (34, 34, 34, 255)
                BORDER    = (50, 50, 50, 120)
                SEPARATOR = (60, 60, 60, 255)
                MENUBAR_BG   = (24, 24, 24, 255)
                MENUBAR_TEXT = (235, 235, 235, 255)
            else:
                BG        = (248, 248, 248, 255)
                BG_CARD   = (255, 255, 255, 255)
                TEXT      = (20, 20, 20, 255)      # <- dark body text in light mode
                MUTED     = (90, 90, 90, 255)
                FRAME     = (235, 235, 235, 255)
                FRAME_H   = (225, 225, 225, 255)
                FRAME_A   = (215, 215, 215, 255)
                BORDER    = (0, 0, 0, 40)
                SEPARATOR = (200, 200, 200, 255)
                MENUBAR_BG   = (255, 255, 255, 255)
                MENUBAR_TEXT = (20, 20, 20, 255)

            with dpg.theme(tag=tag):
                # ---------- Global ----------
                with dpg.theme_component(dpg.mvAll):
                    dpg.add_theme_style(dpg.mvStyleVar_WindowRounding,    12)
                    dpg.add_theme_style(dpg.mvStyleVar_FrameRounding,     10)
                    dpg.add_theme_style(dpg.mvStyleVar_PopupRounding,     10)
                    dpg.add_theme_style(dpg.mvStyleVar_TabRounding,        8)
                    dpg.add_theme_style(dpg.mvStyleVar_ScrollbarRounding,  9)
                    dpg.add_theme_style(dpg.mvStyleVar_WindowPadding,     14, 12)
                    dpg.add_theme_style(dpg.mvStyleVar_FramePadding,      10, 8)
                    dpg.add_theme_style(dpg.mvStyleVar_ItemSpacing,       10, 8)
                    dpg.add_theme_style(dpg.mvStyleVar_ItemInnerSpacing,   8, 6)
                    dpg.add_theme_style(dpg.mvStyleVar_IndentSpacing,     16)

                    # Global text color (light = dark text; dark = light text)
                    dpg.add_theme_color(dpg.mvThemeCol_Text, TEXT)
                    dpg.add_theme_color(dpg.mvThemeCol_WindowBg, BG)
                    dpg.add_theme_color(dpg.mvThemeCol_Border, BORDER)
                    dpg.add_theme_color(dpg.mvThemeCol_Separator, SEPARATOR)
                    dpg.add_theme_color(dpg.mvThemeCol_FrameBg, FRAME)
                    dpg.add_theme_color(dpg.mvThemeCol_FrameBgHovered, FRAME_H)
                    dpg.add_theme_color(dpg.mvThemeCol_FrameBgActive, FRAME_A)
                    dpg.add_theme_color(dpg.mvThemeCol_CheckMark, accent)
                    dpg.add_theme_color(dpg.mvThemeCol_Header, FRAME)
                    dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, FRAME_H)
                    dpg.add_theme_color(dpg.mvThemeCol_HeaderActive, FRAME_A)
                    dpg.add_theme_color(dpg.mvThemeCol_ScrollbarGrab, FRAME_H)
                    dpg.add_theme_color(dpg.mvThemeCol_ScrollbarGrabHovered, FRAME_A)
                    dpg.add_theme_color(dpg.mvThemeCol_ScrollbarGrabActive, FRAME_A)

                # ---------- Menubar (File/Edit/View/Run/Help) ----------
                with dpg.theme_component(dpg.mvMenuBar):
                    dpg.add_theme_color(dpg.mvThemeCol_MenuBarBg, MENUBAR_BG)
                    dpg.add_theme_color(dpg.mvThemeCol_Text,      MENUBAR_TEXT)
                    dpg.add_theme_color(dpg.mvThemeCol_Separator, SEPARATOR)
                    dpg.add_theme_color(dpg.mvThemeCol_Border,    BORDER)
                with dpg.theme_component(dpg.mvMenuItem):
                    dpg.add_theme_color(dpg.mvThemeCol_Text,          MENUBAR_TEXT)
                    dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered,  FRAME_H)
                    dpg.add_theme_color(dpg.mvThemeCol_HeaderActive,   FRAME_A)

                # ---------- Child windows look like cards ----------
                with dpg.theme_component(dpg.mvChildWindow):
                    dpg.add_theme_color(dpg.mvThemeCol_ChildBg, BG_CARD)
                    dpg.add_theme_color(dpg.mvThemeCol_Border,  BORDER)

                # ---------- Buttons (blue accent) ----------
                hover  = (min(accent[0]+10,255), min(accent[1]+20,255), min(accent[2]+20,255), 255)
                active = (max(accent[0]-10,0),   max(accent[1]-10,0),   max(accent[2]-20,0),   255)
                with dpg.theme_component(dpg.mvButton):
                    dpg.add_theme_color(dpg.mvThemeCol_Button,        accent)
                    dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, hover)
                    dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  active)
                    dpg.add_theme_color(dpg.mvThemeCol_Text,          (255, 255, 255, 255))
                    dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 10)
                    dpg.add_theme_style(dpg.mvStyleVar_FramePadding,  10, 8)

        make("theme_dark",  "dark")
        make("theme_light", "light")

    def bind_tab_text_white_to_all_tab_bars():
        """
        Keeps ONLY tab labels white, regardless of base theme.
        Bind this after your tab bars are created.
        """
        if dpg.does_item_exist("__theme_tabtext_white__"):
            dpg.delete_item("__theme_tabtext_white__")
        with dpg.theme(tag="__theme_tabtext_white__"):
            with dpg.theme_component(dpg.mvTab):
                dpg.add_theme_color(dpg.mvThemeCol_Text, (255, 255, 255, 255))

        # Bind to every TabBar found (so both sets get it automatically)
        for item in dpg.get_all_items():
            info = dpg.get_item_info(item)
            if info and info.get("type") == "mvAppItemType::mvTabBar":
                dpg.bind_item_theme(item, "__theme_tabtext_white__")


    def apply_theme(mode: str = "auto"):
        """Bind dark/light theme and your UI font."""
        if mode == "auto":
            mode = (SET.get("THEME") or "dark")
            if mode == "auto":
                mode = "dark"
        dpg.bind_theme("theme_dark" if mode == "dark" else "theme_light")
        if fonts.get("ui"):
            dpg.bind_font(fonts["ui"])
        # Defer the tab text binding so it runs after UI is built
        try:
            dpg.set_frame_callback(2, _bind_tab_text_white_to_all_tab_bars)
        except Exception:
            pass

    def maximize_on_first_run():
        """
        Open the app at 100% width/height (maximize the viewport).
        Call this right after dpg.show_viewport().
        """
        try:
            dpg.maximize_viewport()
        except Exception:
            # fallback: toggle fullscreen twice (enter/exit) to force a maximize-like state
            try:
                dpg.toggle_viewport_fullscreen()
                dpg.toggle_viewport_fullscreen()
            except Exception:
                pass
    # ---- Build & apply once at startup ----
    build_modern_themes()                 # base themes
    apply_theme(SET.get("THEME", "auto")) # bind current theme

    def _fit_root_to_viewport(margin: int = 16):
        """Make the 'root' window fill the viewport (with a small margin)."""
        if not dpg.does_item_exist("root"):
            return

        # Prefer client area (excludes menu bar). Fall back to full size if needed.
        try:
            vw = dpg.get_viewport_client_width()
            vh = dpg.get_viewport_client_height()
        except Exception:
            vw = vh = 0

        if not vw or not vh:
            try:
                vw = dpg.get_viewport_width()
                vh = dpg.get_viewport_height()
            except Exception:
                vw, vh = 1320, 860  # sensible default

        top_margin = 36  # space under the viewport menu bar
        dpg.configure_item(
            "root",
            pos=(margin, top_margin),
            width=max(960, int(vw - 2 * margin)),
            height=max(640, int(vh - top_margin - margin)),
        )

    if not dpg.does_item_exist("theme_btn_primary"):
        with dpg.theme(tag="theme_btn_primary"):
            with dpg.theme_component(dpg.mvButton):
                dpg.add_theme_color(dpg.mvThemeCol_Button,        (56, 126, 245, 255))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (76, 146, 255, 255))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  (46, 106, 215, 255))
                dpg.add_theme_color(dpg.mvThemeCol_Text,          (255, 255, 255, 255))
                dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 10)
                dpg.add_theme_style(dpg.mvStyleVar_FramePadding,  10, 8)

    # --- Apply helper -----------------------------------------------------------
    def apply_theme(mode: str = "auto"):
        if mode == "auto":
            mode = (SET.get("THEME") or "dark")
            if mode == "auto":
                mode = "dark"
        dpg.bind_theme("theme_dark" if mode == "dark" else "theme_light")
        if fonts.get("ui"):
            dpg.bind_font(fonts["ui"])

    # Apply on launch
    apply_theme(SET.get("THEME", "auto"))

    # viewport + menu bar
    dpg.create_viewport(title=APP_NAME, width=1320, height=860)
    with dpg.viewport_menu_bar():
        with dpg.menu(label="File"):
            dpg.add_menu_item(label="Exit", callback=lambda: dpg.stop_dearpygui())
        with dpg.menu(label="Edit"):
            dpg.add_menu_item(label="Open Repo Folder", callback=lambda: _open_path(REPO_PATH))
            dpg.add_menu_item(label="Open Video Preview",     callback=lambda: _open_path(PREVIEW_DIR))
        with dpg.menu(label="View"):
            dpg.add_menu_item(label="Auto",  callback=lambda: (SET.update(THEME="auto"),  save_settings(SET), apply_theme("auto")))
            dpg.add_menu_item(label="Darkness",  callback=lambda: (SET.update(THEME="dark"),  save_settings(SET), apply_theme("dark")))
            dpg.add_menu_item(label="Brightness", callback=lambda: (SET.update(THEME="light"), save_settings(SET), apply_theme("light")))
        with dpg.menu(label="Run"):
            dpg.add_menu_item(label="YouTube Bulk Editor", callback=lambda: dpg.set_value("tabs", "tab_bulk"))
            dpg.add_menu_item(label="YouTube | Website Sync", callback=lambda: dpg.set_value("tabs", "tab_cache"))
            dpg.add_menu_item(label="Update Your Website", callback=lambda: dpg.set_value("tabs", "tab_update"))
            dpg.add_menu_item(label="Channel Insights", callback=lambda: dpg.set_value("tabs", "tab_insights"))
        with dpg.menu(label="Help"):
            dpg.add_menu_item(label="View Logs", callback=lambda: dpg.set_value("tabs", "tab_logs"))
            dpg.add_menu_item(label="Settings", callback=lambda: dpg.set_value("tabs", "tab_settings"))
            dpg.add_menu_item(label="About", callback=lambda: dpg.show_item("about_modal"))
  
    # ===================== HEADER (split L/R, informative) =====================

    def build_header():
        dpg.add_spacer(height=28)

# -------- 2025-style compact status row (color-coded) --------
        with dpg.group(horizontal=True):
            # YouTube connectivity
            dpg.add_text("●", tag="hdr_conn_dot", color=_status_color("error"))
            dpg.add_text("YouTube: Not connected", tag="hdr_conn_text")
            dpg.add_spacer(width=18)

            # Site reachability
            dpg.add_text("●", tag="hdr_site_dot", color=_status_color("muted"))
            dpg.add_text("Site: —", tag="hdr_site_text")
            dpg.add_spacer(width=18)

            # Git quick facts
            dpg.add_text("Git:", color=(180,180,180,255))
            dpg.add_text("—", tag="hdr_git_branch")
            dpg.add_text("•", color=(120,120,120,255))
            dpg.add_text("— changes", tag="hdr_git_changes")
            dpg.add_text("•", color=(120,120,120,255))
            dpg.add_text("Last commit —", tag="hdr_git_last")

        dpg.add_spacer(height=6); dpg.add_separator(); dpg.add_spacer(height=6)

        with dpg.group(horizontal=True):
            # LEFT column (side-by-side mini-columns)
            with dpg.group(horizontal=True):
                # Channel (label over value)
                with dpg.group():
                    dpg.add_text("Channel", color=(120, 120, 120, 255))
                    dpg.add_text(CHANNEL_HANDLE or "—", tag="hdr_channel")

                dpg.add_spacer(width=24)

                # Quota (label over value)
                with dpg.group():
                    dpg.add_text("YouTube Quota", color=(120, 120, 120, 255))
                    dpg.add_progress_bar(tag="hdr_quota_bar", default_value=0.0,
                                        overlay="0 / 0", height=16, width=260)
                    dpg.add_text("", tag="quota_text_gui")

            # Status row (YouTube / Site / Git) — always render
            if not dpg.does_item_exist("hdr_status_row"):
                with dpg.group(horizontal=True, parent="hdr_left", tag="hdr_status_row"):
                    dpg.add_text("Status", color=(120,120,120,255))
                    # YouTube connectivity
                    dpg.add_color_button(tag="hdr_conn_dot", default_value=(160,160,160,255),
                                        no_alpha=True, width=12, height=12)
                    dpg.add_text("", tag="hdr_conn_text")
                    dpg.add_spacer(width=12)
                    # Site reachability
                    dpg.add_color_button(tag="hdr_site_dot", default_value=(160,160,160,255),
                                        no_alpha=True, width=12, height=12)
                    dpg.add_text("", tag="hdr_site_text")
                    dpg.add_spacer(width=12)
                    # Git summary
                    dpg.add_text("•", color=(120,120,120,255))
                    dpg.add_text("", tag="hdr_git_changes")
                    dpg.add_text("•", color=(120,120,120,255))
                    dpg.add_text("", tag="hdr_git_branch")
                    dpg.add_text("•", color=(120,120,120,255))
                    dpg.add_text("", tag="hdr_git_last")
            else:
                dpg.show_item("hdr_status_row")

            # paint initial values

            dpg.add_spacer(width=40)  # gap

            # RIGHT: Repo + Site + Cache count
            with dpg.group(tag="hdr_right"):
                dpg.add_text("Repo", color=(120,120,120,255))
                dpg.add_text(REPO_PATH or "—", tag="hdr_repo")
                dpg.add_spacer(height=2)
                dpg.add_text("Site", color=(120,120,120,255))
                dpg.add_text(SITE_URL or "—", tag="hdr_site")
                dpg.add_spacer(height=2)
                dpg.add_text("", tag="hdr_videos", color=(120,120,120,255))
            _inject_status_row()

        dpg.add_spacer(height=8); dpg.add_separator(); dpg.add_spacer(height=8)

    def refresh_header():
        """Update the header metrics (quota bar, cache count, etc.)."""
        # 1) Update the quota text using your existing function (if available)
        try:
            _reload_quota_text()  # keeps quota_text_gui up to date
        except Exception:
            pass

        # 2) Parse numbers out of quota_text_gui to drive the progress bar
        used, total = 0, 0
        try:
            txt = dpg.get_value("quota_text_gui") or ""
            # expected like: "Quota (est): Used 10000 / 10000"
            import re
            m = re.search(r'(\d[\d,]*)\s*/\s*(\d[\d,]*)', txt)
            if m:
                used  = int(m.group(1).replace(",", ""))
                total = int(m.group(2).replace(",", ""))
        except Exception:
            pass

        try:
            pct = (used / total) if total else 0.0
            pct = max(0.0, min(1.0, pct))
            if dpg.does_item_exist("hdr_quota_bar"):
                dpg.set_value("hdr_quota_bar", pct)
                dpg.configure_item("hdr_quota_bar", overlay=f"{used:,} / {total:,}")
        except Exception:
            pass

        # 3) Show videos in cache
        try:
            vids = load_cache() or []
            dpg.set_value("hdr_videos", f"Videos in cache: {len(vids):,}")
        except Exception:
            dpg.set_value("hdr_videos", "Videos in cache: —")

        # 4) Ensure the core labels stay current (in case globals changed)
        if dpg.does_item_exist("hdr_channel"):
            dpg.set_value("hdr_channel", CHANNEL_HANDLE or "—")
        if dpg.does_item_exist("hdr_repo"):
            dpg.set_value("hdr_repo", REPO_PATH or "—")
        if dpg.does_item_exist("hdr_site"):
            dpg.set_value("hdr_site", SITE_URL or "—")

    with dpg.window(tag="root",
                no_title_bar=True, no_move=True, no_resize=True, no_collapse=True,
                no_scrollbar=True):   # <-- prevent the root from becoming a big scroll box
        dpg.add_spacer(height=28)  # push under menu bar

        # Two-column header, no child_windows (so no little scroll areas)
        with dpg.group(horizontal=True):
            # LEFT: Channel + Quota
            with dpg.group():
                with dpg.group(horizontal=True):
                    dpg.add_text("YouTube Channel:")
                    dpg.add_text(CHANNEL_HANDLE or "—", tag="hdr_channel")
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    dpg.add_text("", tag="quota_text_gui")  # keep your existing line for details

            dpg.add_spacer(width=40)  # gap between columns

            # RIGHT: Repo + Site + cache count
            with dpg.group():
                with dpg.group(horizontal=True):
                    dpg.add_text("Local Repo:")
                    dpg.add_text(REPO_PATH or "—", tag="hdr_repo")
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    dpg.add_text("Your Website:")
                    dpg.add_text(SITE_URL or "—", tag="hdr_site")
                dpg.add_spacer(height=8)
                dpg.add_text("", tag="hdr_videos")  

        # ===== Tabs =====
        with dpg.tab_bar(tag="tabs"):
            # ---------- Bulk Editor (OAuth) ----------
            BE_SCOPES = [
                "https://www.googleapis.com/auth/youtube",           # write scope
                "https://www.googleapis.com/auth/youtube.force-ssl"  # keep existing
            ]

            BE_CACHE_FILE   = "be_videos_cache.json"
            BE_CACHE_EXPIRY = 86400  # 24h

            BE = {
                "youtube": None,
                "channel_id": "",
                "channel_title": "",
                "uploads_playlist": "",
                "videos": [],           # list of full video JSONs from API
                "filtered": [],         # filtered subset for UI
                "selected": set(),      # row indices in 'filtered'
            }

            def be_add_quota(method:str, mult:int=1):
                try:
                    add_quota_usage(method, mult)
                except Exception:
                    pass
                _reload_quota_text()

            def _be_log(msg:str):
                _append_log("be_log_text", msg)

            try:
                from google.oauth2.credentials import Credentials
                from google.auth.transport.requests import Request
                from google.auth.exceptions import RefreshError
            except Exception as e:
                _missing.append(("google-auth", str(e)))
                Credentials = Request = None
                RefreshError = Exception

            def be_get_service(token_file='token.json'):
                if 'google-auth' in [m[0] for m in _missing]:
                    _be_log("Google auth libraries missing. Install: pip install google-api-python-client google-auth-oauthlib")
                    return None
                # Prefer JSON tokens so we can refresh without reauth
                creds = None

                if os.path.exists(token_file):
                    try:
                        creds = Credentials.from_authorized_user_file(token_file, BE_SCOPES)
                        # If token is missing required scopes, force re-auth below
                        if not creds or not set(BE_SCOPES).issubset(set(creds.scopes or [])):
                            creds = None
                    except Exception:
                        creds = None

                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        try:
                            # silent refresh, no browser
                            creds.refresh(Request())
                        except RefreshError:
                            # token revoked/expired -> force full re-auth
                            creds = None
                    if not creds or not creds.valid:
                        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", BE_SCOPES)
                        creds = flow.run_local_server(port=0)
                    # persist
                    with open(token_file, "w", encoding="utf-8") as fh:
                        fh.write(creds.to_json())

                return build("youtube", "v3", credentials=creds, cache_discovery=False)

            def be_connect_account():
                try:
                    BE["youtube"] = be_get_service()
                    if BE["youtube"] is None:
                        return
                    r = BE["youtube"].channels().list(part="snippet,contentDetails", mine=True).execute()

                    be_add_quota("channels.list")
                    it = (r.get("items") or [])[0]
                    BE["channel_id"] = it["id"]
                    BE["channel_title"] = it["snippet"]["title"]
                    BE["uploads_playlist"] = it["contentDetails"]["relatedPlaylists"]["uploads"]  # <- set here only
                    dpg.set_value("be_account_label", f"Connected: {BE['channel_title']}")
                    _reload_quota_text()  # refresh used number immediately after login
                    be_fetch_all_videos(force_refresh=False)

                except HttpError as e:
                    if _is_quota_exceeded(e):
                        _quota_trip_ui("account connect")
                    else:
                        _be_log(f"Connection error: {e}")

            def be_switch_account():
                try:
                    for f in ("token.json", "token.pickle"):
                        if os.path.exists(f):
                            os.remove(f)
                except Exception:
                    pass
                be_connect_account()

            def be_fetch_all_videos(force_refresh=False):
                BE["videos"] = []

                # Try cache first unless we are forced — but even on force, we’ll fall back if quota hits.
                cache_ok = False
                if not force_refresh and os.path.exists(BE_CACHE_FILE):
                    try:
                        if time.time() - os.path.getmtime(BE_CACHE_FILE) < BE_CACHE_EXPIRY:
                            with open(BE_CACHE_FILE, "r", encoding="utf-8") as f:
                                BE["videos"] = json.load(f)
                            cache_ok = True
                            _be_log("Loaded videos from cache.")
                    except Exception:
                        cache_ok = False

                if cache_ok:
                    be_apply_filter("")  # seed table
                    return

                # Ensure we have a service and the uploads playlist id
                if not BE.get("youtube"):
                    be_connect_account()
                    if not BE.get("youtube"):
                        return

                uploads_id = BE.get("uploads_playlist", "")
                if not uploads_id:
                    _be_log("Cannot fetch: uploads playlist is unknown (likely quota error earlier).")
                    # best-effort fallback to cache even if stale
                    try:
                        with open(BE_CACHE_FILE, "r", encoding="utf-8") as f:
                            BE["videos"] = json.load(f)
                        _be_log("Loaded stale cache due to missing uploads playlist.")
                        be_apply_filter("")
                        return
                    except Exception:
                        _quota_trip_ui("playlist discovery")
                        return

                yt = BE["youtube"]
                next_token = None
                try:
                    while True:
                        req = yt.playlistItems().list(
                            part="contentDetails", playlistId=uploads_id, maxResults=50, pageToken=next_token
                        )
                        try:
                            resp = req.execute()
                            be_add_quota("playlistItems.list")
                        except HttpError as e:
                            if _is_quota_exceeded(e):
                                _quota_trip_ui("playlistItems.list")
                                # fallback to cache
                                try:
                                    with open(BE_CACHE_FILE, "r", encoding="utf-8") as f:
                                        BE["videos"] = json.load(f)
                                    be_apply_filter("")
                                except Exception:
                                    pass
                                return
                            raise

                        ids = [it["contentDetails"]["videoId"] for it in resp.get("items", [])]
                        for i in range(0, len(ids), 50):
                            batch = ids[i:i+50]
                            try:
                                vresp = yt.videos().list(part="snippet,status,recordingDetails", id=",".join(batch)).execute()
                                be_add_quota("videos.list")
                            except HttpError as e:
                                if _is_quota_exceeded(e):
                                    _quota_trip_ui("videos.list")
                                    # keep whatever we have and fall back to cache merge
                                    try:
                                        with open(BE_CACHE_FILE, "r", encoding="utf-8") as f:
                                            cached = json.load(f)
                                        # merge minimal: keep newly fetched first
                                        BE["videos"].extend(cached)
                                    except Exception:
                                        pass
                                    be_apply_filter("")
                                    return
                                raise
                            BE["videos"].extend(vresp.get("items", []))

                        next_token = resp.get("nextPageToken")
                        if not next_token:
                            break

                    # save cache on success
                    try:
                        with open(BE_CACHE_FILE, "w", encoding="utf-8") as f:
                            json.dump(BE["videos"], f)
                    except Exception:
                        pass

                    _be_log(f"Fetched {len(BE['videos'])} videos.")
                    be_apply_filter("")
                except Exception as e:
                    _be_log(f"Fetch error: {e}")
                    _quota_trip_ui("fetch")


            def be_apply_filter(q:str):
                q = (q or "").lower().strip()
                if not q:
                    BE["filtered"] = BE["videos"][:]
                else:
                    out = []
                    for v in BE["videos"]:
                        t = (v["snippet"].get("title") or "").lower()
                        d = (v["snippet"].get("description") or "").lower()
                        if q in t or q in d:
                            out.append(v)
                    BE["filtered"] = out
                BE["selected"] = set()
                be_render_table()

            def be_render_table():
                if "dearpygui" in [m[0] for m in _missing]:
                    return
                if not dpg.does_item_exist("be_table"):
                    return

                dpg.delete_item("be_table", children_only=True)

                dpg.add_table_column(label="Pick",    parent="be_table", width_fixed=True)
                dpg.add_table_column(label="Title",   parent="be_table", init_width_or_weight=3)
                dpg.add_table_column(label="Privacy", parent="be_table", width_fixed=True)
                dpg.add_table_column(label="Category",parent="be_table", width_fixed=True)
                dpg.add_table_column(label="ID",      parent="be_table", width_fixed=True)

                for i, v in enumerate(BE["filtered"]):
                    title   = v["snippet"].get("title","")
                    privacy = v.get("status",{}).get("privacyStatus","—")
                    cat     = CATEGORIES.get(v["snippet"].get("categoryId",""), "Unknown")
                    vid     = v.get("id","")
                    with dpg.table_row(parent="be_table"):
                        dpg.add_checkbox(
                            tag=f"be_pick_{i}",
                            default_value=(i in BE["selected"]),
                            callback=lambda s, a, u=i: be_toggle_pick(u)
                        )
                        dpg.add_text(title[:80]); dpg.add_text(privacy); dpg.add_text(cat); dpg.add_text(vid)

            def be_toggle_pick(i:int):
                if i in BE["selected"]:
                    BE["selected"].remove(i)
                else:
                    BE["selected"].add(i)

            def be_select_all():
                BE["selected"] = set(range(len(BE["filtered"])))
                be_render_table()

            def be_clear_sel():
                BE["selected"].clear()
                be_render_table()

            # ---- Text transforms ----
            def be_apply_desc_action(desc, action, find, replace, keyword, use_regex=False, match_case=False):
                desc    = _norm_newlines(desc or "")
                find    = _norm_newlines(find or "")
                replace = _norm_newlines(replace or "")
                keyword = _norm_newlines(keyword or "")
                import re

                if action == "Find & Replace":
                    if not find:
                        return desc
                    if use_regex:
                        flags = 0 if match_case else re.IGNORECASE
                        try:
                            return re.sub(find, replace, desc, flags=flags)
                        except re.error:
                            return desc  # bad regex -> no change
                    else:
                        if match_case:
                            return desc.replace(find, replace)
                        return re.sub(re.escape(find), replace, desc, flags=re.IGNORECASE)

                if action == "Prepend":
                    return replace + desc

                if action == "Append":
                    return desc + replace

                if action == "Replace After Keyword" and keyword and keyword in desc:
                    pos = desc.find(keyword) + len(keyword)
                    return desc[:pos] + replace

                if action == "Replace Before Keyword" and keyword and keyword in desc:
                    pos = desc.find(keyword)
                    return replace + desc[pos:]

                return desc

            def be_apply_title_action(title, action, text):
                title = title or ""
                if action == "None": return title
                if action == "Prepend": return (text or "") + title
                if action == "Append":  return title + (text or "")
                if action == "Replace": return (text or "") or title
                return title

            def be_apply_tag_action(tags, action, text):
                tags = list(tags or [])
                if action == "None": return tags
                new = [t.strip() for t in (text or "").split(",") if t.strip()]
                if action == "Add": tags.extend(new)
                elif action == "Replace": tags = new
                seen=set(); out=[]
                for t in tags:
                    if t not in seen:
                        out.append(t); seen.add(t)
                return out

            def be_read_controls():
                return {
                    "action": dpg.get_value("be_desc_action"),
                    "find": dpg.get_value("be_find") or "",
                    "replace": dpg.get_value("be_replace") or "",
                    "keyword": dpg.get_value("be_keyword") or "",
                    "title_action": dpg.get_value("be_title_action"),
                    "title_text": dpg.get_value("be_title_text") or "",
                    "tag_action": dpg.get_value("be_tag_action"),
                    "tag_text": dpg.get_value("be_tag_text") or "",
                    "category": dpg.get_value("be_category") or "no_change",
                    "privacy": dpg.get_value("be_privacy") or "no_change",
                    "license": dpg.get_value("be_license") or "no_change",
                    "embeddable": dpg.get_value("be_embeddable") or "no_change",
                    "public_stats": dpg.get_value("be_public_stats") or "no_change",
                    "made_for_kids": dpg.get_value("be_mfk") or "no_change",
                    "language": dpg.get_value("be_language") or "",
                    "recording": dpg.get_value("be_recording") or "No Change",
                    "thumbnail": dpg.get_value("be_thumb_path") or "",
                    # NEW:
                    "use_regex": dpg.get_value("be_find_regex") or False,
                    "match_case": dpg.get_value("be_find_match_case") or False,
                }

            def be_preview_changes():
                be_progress(0.02, "Building preview…")
                try:
                    if not BE["selected"]:
                        _be_log("Select some videos first (Bulk Editor → checkboxes).")
                        try:
                            notify("Mayhem Maker", "No videos selected — pick some first.")
                            dpg.show_item("tab_logs")
                        except Exception:
                            pass
                        return

                    opts = be_read_controls()
                    dpg.set_value("be_preview_text", "")

                    sel = sorted(BE["selected"])
                    total = len(sel)

                    for idx, i in enumerate(sel, start=1):
                        v  = BE["filtered"][i]
                        sn = v["snippet"]
                        st = v.get("status", {})
                        rd = v.get("recordingDetails", {})

                        original_title = sn.get("title", "")
                        original_desc  = sn.get("description", "") or ""
                        new_title = be_apply_title_action(original_title, opts["title_action"], opts["title_text"])
                        new_desc  = be_apply_desc_action(
                            original_desc, opts["action"], opts["find"], opts["replace"], opts["keyword"],
                            use_regex=opts["use_regex"], match_case=opts["match_case"]
                        )
                        original_tags = sn.get("tags", [])
                        new_tags = be_apply_tag_action(original_tags, opts["tag_action"], opts["tag_text"])

                        lines = [f"Video: {original_title}"]
                        cat_new_id = _cat_id_from_selection(opts["category"])
                        if cat_new_id and cat_new_id != sn.get("categoryId"):
                            lines.append(
                                f"Category: {CATEGORIES.get(sn.get('categoryId',''),'Unknown')} -> {CATEGORIES.get(cat_new_id,'Unknown')}"
                            )
                        if new_title != original_title:
                            lines.append(f"Title: {original_title} -> {new_title}")
                        if new_desc != original_desc:
                            diff = "\n".join(difflib.ndiff(original_desc.splitlines(), new_desc.splitlines()))
                            lines.append("Description changes:\n" + diff)
                        if set(new_tags) != set(original_tags):
                            lines.append(f"Tags: {', '.join(original_tags)} -> {', '.join(new_tags)}")
                        if opts["privacy"] != "no_change" and opts["privacy"] != st.get("privacyStatus"):
                            lines.append(f"Privacy: {st.get('privacyStatus','N/A')} -> {opts['privacy']}")
                        if opts["license"] != "no_change" and opts["license"] != st.get("license"):
                            lines.append(f"License: {st.get('license','N/A')} -> {opts['license']}")
                        if opts["embeddable"] != "no_change" and opts["embeddable"] != st.get("embeddable"):
                            lines.append(f"Embeddable: {st.get('embeddable','N/A')} -> {opts['embeddable']}")
                        if opts["public_stats"] != "no_change" and opts["public_stats"] != st.get("publicStatsViewable"):
                            lines.append(f"Public Stats: {st.get('publicStatsViewable','N/A')} -> {opts['public_stats']}")
                        if opts["made_for_kids"] != "no_change" and opts["made_for_kids"] != st.get("madeForKids"):
                            lines.append(f"Made for Kids: {st.get('madeForKids','N/A')} -> {opts['made_for_kids']}")
                        if opts["language"] and opts["language"] != sn.get("defaultLanguage"):
                            lines.append(f"Language: {sn.get('defaultLanguage','N/A')} -> {opts['language']}")
                        if opts["recording"] != "No Change" and opts["recording"] != rd.get("recordingDate"):
                            lines.append(f"Recording Date: {rd.get('recordingDate','N/A')} -> {opts['recording']}")
                        if opts["thumbnail"]:
                            lines.append(f"Thumbnail: {opts['thumbnail']}")

                        existing = dpg.get_value("be_preview_text") or ""
                        dpg.set_value("be_preview_text", existing + "\n".join(lines) + "\n\n")

                        # preview progress across items
                        be_progress(0.05 + 0.9 * (idx / total), f"Preview {idx}/{total}")

                    be_progress(1.0, "Preview ready")
                except Exception as e:
                    _be_log(f"Preview error: {e}")
                    be_progress(1.0, "Preview failed")

            def be_dry_run():
                be_progress(0.02, "Dry run")
                if not BE["selected"]:
                    _be_log("Select some videos first.")
                    be_progress(1.0, "Dry run complete")
                    return
                opts = be_read_controls()
                changes_list = []
                for i in sorted(BE["selected"]):
                    v  = BE["filtered"][i]
                    sn = v["snippet"]
                    st = v.get("status",{})
                    rd = v.get("recordingDetails",{})

                    original_title = sn.get("title","")
                    original_desc  = sn.get("description","") or ""   # <-- define this
                    new_title = be_apply_title_action(original_title, opts["title_action"], opts["title_text"])
                    new_desc  = be_apply_desc_action(
                        original_desc, opts["action"], opts["find"], opts["replace"], opts["keyword"],
                        use_regex=opts["use_regex"], match_case=opts["match_case"]
                    )
                    original_tags = sn.get("tags",[])
                    new_tags = be_apply_tag_action(original_tags, opts["tag_action"], opts["tag_text"])

                    change = {}
                    cat_new_id = _cat_id_from_selection(opts["category"])
                    if cat_new_id and cat_new_id != sn.get("categoryId"): change["categoryId"] = cat_new_id
                    if new_title != original_title: change["title"] = new_title
                    if new_desc  != original_desc:  change["description"] = new_desc
                    if set(new_tags) != set(original_tags): change["tags"] = new_tags
                    if opts["privacy"]!="no_change" and opts["privacy"]!=st.get("privacyStatus"): change["privacyStatus"] = opts["privacy"]
                    if opts["license"]!="no_change" and opts["license"]!=st.get("license"): change["license"] = opts["license"]
                    if opts["embeddable"]!="no_change" and opts["embeddable"]!=st.get("embeddable"): change["embeddable"] = opts["embeddable"]
                    if opts["public_stats"]!="no_change" and opts["public_stats"]!=st.get("publicStatsViewable"): change["publicStatsViewable"] = opts["public_stats"]
                    if opts["made_for_kids"]!="no_change" and opts["made_for_kids"]!=st.get("madeForKids"): change["madeForKids"] = opts["made_for_kids"]
                    if opts["language"] and opts["language"] != sn.get("defaultLanguage"): change["defaultLanguage"] = opts["language"]
                    if opts["recording"] != "No Change" and opts["recording"] != rd.get("recordingDate"): change["recordingDate"] = opts["recording"]
                    if opts["thumbnail"]: change["thumbnail"] = opts["thumbnail"]
                    if change:
                        changes_list.append({"id": v["id"], "changes": change})
                with open("dry_run.json","w",encoding="utf-8") as f:
                    json.dump(changes_list, f, indent=2)
                _be_log("Dry run complete -> dry_run.json")

            def be_update_videos():
                if not BE["selected"]:
                    _be_log("Select some videos first.")
                    return
                if remaining_quota() < 50:
                    _be_log("Quota low; cannot update safely.")
                    return
                yt = BE.get("youtube")
                if not yt:
                    _be_log("Not connected.")
                    return

                opts = be_read_controls()
                errs = []
                total = len(BE["selected"])
                done  = 0

                # kick off progress
                be_progress(0.01, f"Starting… 0/{total}")

                try:
                    for idx, i in enumerate(sorted(BE["selected"]), start=1):
                        v  = BE["filtered"][i]
                        sn = v["snippet"]
                        st = v.get("status",{})
                        rd = v.get("recordingDetails",{})
                        vid = v["id"]

                        original_title = sn.get("title","")
                        original_desc  = sn.get("description","") or ""
                        new_title = be_apply_title_action(original_title, opts["title_action"], opts["title_text"])
                        new_desc  = be_apply_desc_action(
                            original_desc, opts["action"], opts["find"], opts["replace"], opts["keyword"],
                            use_regex=opts["use_regex"], match_case=opts["match_case"]
                        )
                        new_tags  = be_apply_tag_action(sn.get("tags",[]), opts["tag_action"], opts["tag_text"])
                        cat_new_id = _cat_id_from_selection(opts["category"])

                        body = {"id": vid}
                        snippet_changed = (
                            new_title != sn.get("title","") or
                            new_desc  != sn.get("description","") or
                            set(new_tags) != set(sn.get("tags",[])) or
                            (cat_new_id and cat_new_id != sn.get("categoryId")) or
                            (opts["language"] and opts["language"] != sn.get("defaultLanguage"))
                        )
                        if snippet_changed:
                            body["snippet"] = {
                                "title": new_title,
                                "description": new_desc,
                                "tags": new_tags,
                                "categoryId": cat_new_id if cat_new_id else sn.get("categoryId"),
                                "defaultLanguage": opts["language"] if opts["language"] else sn.get("defaultLanguage"),
                            }
                        status_changed = any([
                            opts["privacy"]!="no_change" and opts["privacy"]!=st.get("privacyStatus"),
                            opts["license"]!="no_change" and opts["license"]!=st.get("license"),
                            opts["embeddable"]!="no_change" and opts["embeddable"]!=st.get("embeddable"),
                            opts["public_stats"]!="no_change" and opts["public_stats"]!=st.get("publicStatsViewable"),
                            opts["made_for_kids"]!="no_change" and opts["made_for_kids"]!=st.get("madeForKids"),
                        ])
                        if status_changed:
                            body["status"] = {
                                "privacyStatus": opts["privacy"] if opts["privacy"]!="no_change" else st.get("privacyStatus"),
                                "license": opts["license"] if opts["license"]!="no_change" else st.get("license"),
                                "embeddable": True if opts["embeddable"]=="true" else False if opts["embeddable"]=="false" else st.get("embeddable"),
                                "publicStatsViewable": True if opts["public_stats"]=="true" else False if opts["public_stats"]=="false" else st.get("publicStatsViewable"),
                                "madeForKids": True if opts["made_for_kids"]=="true" else False if opts["made_for_kids"]=="false" else st.get("madeForKids"),
                            }
                        if opts["recording"] != "No Change":
                            body["recordingDetails"] = {"recordingDate": opts["recording"]}

                        # progress before firing the API call
                        be_progress(0.05 + 0.9*((idx-1)/total), f"Updating {idx}/{total}: {vid}")

                        try:
                            parts = ",".join([k for k in body.keys() if k!="id"])
                            if parts:
                                yt.videos().update(part=parts, body=body).execute()
                                be_add_quota("videos.update")
                                _be_log(f"Updated {vid}")

                            if opts["thumbnail"]:
                                be_progress(0.05 + 0.9*((idx-1)/total), f"Uploading thumbnail {idx}/{total}: {vid}")
                                mime, _ = mimetypes.guess_type(opts["thumbnail"])
                                media = MediaFileUpload(opts["thumbnail"], mimetype=mime or "image/jpeg")
                                yt.thumbnails().set(videoId=vid, media_body=media).execute()
                                be_add_quota("thumbnails.set")
                                _be_log(f"Thumbnail updated for {vid}")

                            done += 1
                            be_progress(0.05 + 0.9*(idx/total), f"Done {done}/{total}")
                            time.sleep(0.4)  # keep YouTube happy

                        except HttpError as e:
                            errs.append(f"{vid}: {e}")
                            be_progress(0.05 + 0.9*((idx-1)/total), f"Error {idx}/{total} (continuing)")
                        except Exception as e:
                            errs.append(f"{vid}: {e}")
                            be_progress(0.05 + 0.9*((idx-1)/total), f"Error {idx}/{total} (continuing)")

                finally:
                    # refresh and finish either way
                    be_fetch_all_videos(force_refresh=True)
                    _set_text("be_quota_label", f"YouTube Quota (est): Used {estimated_units} / {DAILY_QUOTA_LIMIT}")
                    be_progress(1.0, "Done")

                if errs:
                    with open("update_log.txt","w",encoding="utf-8") as f: f.write("\n".join(errs))
                    _be_log("Some errors -> update_log.txt")

            # === Bulk Editor UI ===
            with dpg.tab(label="YouTube Bulk Editor", tag="tab_bulk"):
                dpg.add_text("Bulk Edit Like a Boss: Titles, Descriptions, Tags, Categories, Privacy & More")
                dpg.add_spacer(height=8)

                with dpg.group(horizontal=True):
                    # Left panel (account + list)
                    with dpg.child_window(width=560, autosize_y=True, border=True):
                        dpg.add_text("", tag="be_account_label")
                        dpg.add_text("", tag="be_quota_label")
                        with dpg.group(horizontal=True):
                            big_button("Connect Channel", lambda: _run_thread(be_switch_account), width=200)
                            big_button("Refresh Videos", lambda: _run_thread(lambda: be_fetch_all_videos(force_refresh=True)), width=200)
                        dpg.add_spacer(height=8)
                        dpg.add_text("Search Your Videos")
                        dpg.add_input_text(hint="Find Anything Fast", tag="be_filter", callback=lambda: be_apply_filter(dpg.get_value("be_filter")), width=-1)
                        dpg.add_spacer(height=8)
                        with dpg.group(horizontal=True):
                            big_button("Select All Videos", be_select_all, width=200)
                            big_button("Clear All Selections", be_clear_sel, width=200)

                        with dpg.table(tag="be_table", header_row=True, resizable=True, policy=dpg.mvTable_SizingStretchProp):
                            pass

                    # Right panel (controls)
                    with dpg.child_window(autosize_x=True, autosize_y=True, border=True):
                        # Title
                        dpg.add_text("Title Action")
                        dpg.add_combo(["None","Prepend","Append","Replace"], tag="be_title_action", width=350)
                        dpg.add_text("Text")
                        dpg.add_input_text(tag="be_title_text", width=-1)
                        dpg.add_spacer(height=8)

                        # Description
                        dpg.add_text("Description Action")
                        dpg.add_combo(["Find & Replace","Prepend","Append","Replace After Keyword","Replace Before Keyword"],
                                    tag="be_desc_action", width=350)
                        dpg.add_text("Find (supports multi-line)")
                        dpg.add_input_text(tag="be_find", multiline=True, height=150, width=-1)
                        dpg.add_spacer(height=8)
                        dpg.add_text("Replace (supports multi-line)")
                        dpg.add_input_text(tag="be_replace", multiline=True, height=150, width=-1)
                        dpg.add_spacer(height=8)
                        with dpg.group(horizontal=True):
                            dpg.add_button(
                                label="Open Large Editor",
                                width=200,
                                callback=lambda: (
                                    dpg.set_value("be_replace_big", dpg.get_value("be_replace") or ""),
                                    dpg.show_item("replace_editor")
                                )
                            )
                        dpg.add_spacer(height=8)
                        dpg.add_checkbox(label="Use Regex", tag="be_find_regex")
                        dpg.add_spacer(height=8)
                        dpg.add_checkbox(label="Match Case", tag="be_find_match_case")
                        dpg.add_spacer(height=8)
                        dpg.add_text("Keyword (Before | After)")
                        dpg.add_input_text(tag="be_keyword", width=-1)
                        dpg.add_spacer(height=8)

                        # Tags
                        dpg.add_text("Tag Action")
                        dpg.add_combo(["None","Add","Replace"], tag="be_tag_action", width=350)
                        dpg.add_text("Tags (comma-separated)")
                        dpg.add_input_text(tag="be_tag_text", width=-1)
                        dpg.add_spacer(height=8)

                        # Other fields (labels above)
                        dpg.add_text("Other Options")
                        dpg.add_text("Category")
                        dpg.add_combo(CATEGORY_CHOICES, tag="be_category", width=350)
                        dpg.add_text("Privacy")
                        dpg.add_combo(["no_change","public","private","unlisted"], tag="be_privacy", width=350)
                        dpg.add_text("License")
                        dpg.add_combo(["no_change","youtube","creativeCommon"], tag="be_license", width=350)
                        dpg.add_text("Embeddable")
                        dpg.add_combo(["no_change","true","false"], tag="be_embeddable", width=350)
                        dpg.add_text("Public Stats Viewable")
                        dpg.add_combo(["no_change","true","false"], tag="be_public_stats", width=350)
                        dpg.add_text("Made For Kids")
                        dpg.add_combo(["no_change","true","false"], tag="be_mfk", width=350)
                        dpg.add_text("Default Language (e.g., en)")
                        dpg.add_input_text(tag="be_language", width=350)
                        dpg.add_text("Recording Date (ISO 8601)")
                        dpg.add_input_text(tag="be_recording", default_value="No Change", width=350)

                        # Thumbnail picker (label above)
                        dpg.add_text("Thumbnail Path")
                        with dpg.group(horizontal=True):
                            dpg.add_input_text(tag="be_thumb_path", width=350, readonly=True)
                            big_button("Browse", lambda: dpg.show_item("be_thumb_dialog"), width=200)
                        with dpg.file_dialog(directory_selector=False, show=False,
                                            callback=lambda s,d: dpg.set_value("be_thumb_path", list(d["selections"].values())[0]),
                                            tag="be_thumb_dialog"):
                            dpg.add_file_extension("Image (*.jpg, *.jpeg, *.png){.jpg,.jpeg,.png}")

                        dpg.add_spacer(height=8)
                        with dpg.group(horizontal=True):
                            big_button("Preview Changes",    lambda: _run_thread(be_preview_changes), width=200)
                            big_button("Practice Run",    lambda: _run_thread(be_dry_run), width=200)
                            big_button("Apply to YouTube", lambda: _run_thread(be_update_videos), height=36, width=200)
                            big_button("View Changes",       lambda: _open_path(REPO_PATH), width=200)
                        dpg.add_spacer(height=8)
                        dpg.add_progress_bar(tag="be_progress", width=-1, default_value=0.0, overlay="Ready")
                        dpg.add_spacer(height=8)
                        dpg.add_text("Preview")
                        dpg.add_input_text(tag="be_preview_text", multiline=True, height=350, width=-1, readonly=True)
                        dpg.add_spacer(height=8)
                        dpg.add_text("YouTube Bulk Edit Log")
                        if not dpg.does_item_exist("be_log_text"):
                            dpg.add_input_text(tag="be_log_text", multiline=True, readonly=True, width=-1, height=200)
                        dpg.add_spacer(height=8)

            # ---------- Sync Manager ----------
            with dpg.tab(label="YouTube | Website Sync", tag="tab_cache"):
                dpg.add_text("Sync Your YouTube Channel with Your Website: Fast & Easy")
                dpg.add_spacer(height=8)
                dpg.add_loading_indicator(tag="cache_busy", show=False, radius=12, style=1)
                dpg.add_progress_bar(tag="cache_progress", width=-1, default_value=0.0, overlay="Ready")
                dpg.add_text("", tag="cache_progress_text")

                with dpg.group(tag="cache_controls"):
                    with dpg.group(horizontal=True):
                        big_button("Smart Refresh", lambda: _run_task_cache(lambda: _do_refresh_stale(dpg.get_value("0"))), width=200)
                        big_button("Fetch ALL", lambda: _run_task_cache(_do_fetch_all), width=200)    
                    dpg.add_spacer(height=8); dpg.add_separator(); dpg.add_spacer(height=8)
                    dpg.add_text("Fetch or Refresh by IDs (comma or newline separated)")
                    dpg.add_input_text(tag="ids_box", multiline=True, height=200, width=-1)
                    dpg.add_spacer(height=8)
                    big_button("Refresh These IDs", lambda: _run_task_cache(lambda: _do_refresh_ids(_parse_ids(dpg.get_value("ids_box")))), width=200)
                    dpg.add_spacer(height=8); dpg.add_separator(); dpg.add_spacer(height=8)
                    dpg.add_text("Remove Videos by IDs or Complete YouTube URLs (comma or newline separated)")
                    dpg.add_input_text(tag="remove_ids", multiline=True, height=200, width=-1)
                    dpg.add_spacer(height=8)
                    big_button("Remove & Purge", lambda: _run_task_cache(lambda: _task_remove(_parse_ids(dpg.get_value("remove_ids")))), width=200)
                    dpg.add_spacer(height=8)

            # ---------- Update Site ----------
            with dpg.tab(label="Update Your Website", tag="tab_update"):
                dpg.add_text("Your Website Updated in Minutes, Not Hours: Reliable, Simple, Secure")
                dpg.add_spacer(height=8)
                dpg.add_progress_bar(tag="progress", width=-1, default_value=0.0, overlay="Ready")
                with dpg.group(tag="deploy_progress_wrap", show=False):
                    dpg.add_progress_bar(tag="deploy_progress_bar", default_value=0.0, overlay="0%", width=-1)
                    dpg.add_text("", tag="deploy_progress_text")      
                dpg.add_text("", tag="update_status")   # live status line: “Running… / Complete”
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    dpg.add_checkbox(label="Update Home Page", tag="chk_update_index", default_value=True)
                    dpg.add_checkbox(label="Update Video Pages", tag="chk_update_videos", default_value=True)
                    dpg.add_checkbox(label="Dry-Run (preview only)", tag="chk_dry_run", default_value=False)
                    dpg.add_checkbox(label="Force Rebuild (change all pages)", tag="chk_force", default_value=False)
                dpg.add_spacer(height=8)
                dpg.add_text("Changes Made Message"); dpg.add_input_text(tag="commit_msg", default_value=SET["DEFAULT_COMMIT_MSG"], width=-1)
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    big_button(
                        "Run Update",
                        callback=lambda: (
                            dpg.set_value("confirm_details", _summarize_update_args()),
                            dpg.set_value("progress", 0.0),
                            dpg.configure_item("progress", overlay="Starting…"),
                            dpg.show_item("confirm_modal")
                        ), width=200
                    )
                    big_button("Open Dry-Run Changes", lambda: _open_path(PREVIEW_DIR), width=200)
                dpg.add_spacer(height=8)
                dpg.add_text("Changed Files"); 
                with dpg.child_window(width=-1, height=200, border=False):
                    dpg.add_listbox([], tag="changed_files_list", width=-1, num_items=100)
                dpg.add_spacer(height=8)
                big_button("View First Change", lambda: (dpg.set_value("diff_text", _first_diff_text()), dpg.show_item("diff_modal")), width=200)
                dpg.add_spacer(height=8)
                dpg.add_text("Deploy Output"); dpg.add_input_text(tag="deploy_output", multiline=True, height=200, readonly=True, width=-1)
                dpg.add_spacer(height=8)

            # ---------- Insights ----------
            with dpg.tab(label="Your Channel Insights", tag="tab_insights"):
                dpg.add_text("Your Website Reports, Learn Today: Views, Likes, Uploads, and More")
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    big_button("Reload Insights", lambda: _run_thread(_reload_insights), width=200)
                    dpg.add_text("Videos:"); dpg.add_text("", tag="ins_count")
                    dpg.add_text("|  Total Views:"); dpg.add_text("", tag="ins_views")
                    dpg.add_text("|  Avg Duration (sec):"); dpg.add_text("", tag="ins_avgdur")
                    dpg.add_text("|  Likes:"); dpg.add_text("", tag="ins_likes")
                    dpg.add_text("|  Comments:"); dpg.add_text("", tag="ins_comments")
                    dpg.add_text("|  Mean Views:"); dpg.add_text("", tag="ins_meanviews")
                    dpg.add_text("|  Median Views:"); dpg.add_text("", tag="ins_medianviews")
                dpg.add_spacer(height=8)

                # Existing: Descending views (keep)
                dpg.add_plot(label="Views (descending)", height=300, width=-1, tag="plot_views")
                dpg.add_plot_axis(dpg.mvXAxis, label="Video #", parent="plot_views")
                dpg.add_plot_axis(dpg.mvYAxis, label="Views", parent="plot_views", tag="plot_views_y")
                dpg.add_spacer(height=8)

                # New: Top 20 by views (bar)
                dpg.add_plot(label="Top 20 by Views", height=300, width=-1, tag="plot_top20")
                dpg.add_plot_axis(dpg.mvXAxis, label="Rank (1=highest)", parent="plot_top20")
                dpg.add_plot_axis(dpg.mvYAxis, label="Views", parent="plot_top20", tag="plot_top20_y")
                dpg.add_spacer(height=8)

                # New: Views by Publish Date (old → new)
                dpg.add_plot(label="Views by Publish Date (old → new)", height=300, width=-1, tag="plot_bydate")
                dpg.add_plot_axis(dpg.mvXAxis, label="Chronological Index", parent="plot_bydate")
                dpg.add_plot_axis(dpg.mvYAxis, label="Views", parent="plot_bydate", tag="plot_bydate_y")
                dpg.add_spacer(height=8)

                # Plot 2 — Uploads by Month
                dpg.add_plot(label="Uploads by Month", height=300, width=-1, tag="plot_months")
                dpg.add_plot_axis(dpg.mvXAxis, label="Month", parent="plot_months", tag="plot_months_x")
                dpg.add_plot_axis(dpg.mvYAxis, label="# Uploads", parent="plot_months", tag="plot_months_y")
                dpg.add_spacer(height=8)

            # ---------- Logs ----------
            with dpg.tab(label="View Change Logs", tag="tab_logs"):
                dpg.add_text("Martocci Mayhem Log Reports, Changes, Errors, and Processes in Detail")
                dpg.add_spacer(height=8)
                with dpg.group(horizontal=True):
                    big_button("Clear Logs", lambda: (_set_text("logs_text", ""), _set_text("be_log_text", "")), width=200)
                    big_button("Copy Logs",  lambda: dpg.set_clipboard_text((dpg.get_value("logs_text") or "").strip()), width=200)
                dpg.add_spacer(height=8)
                if not dpg.does_item_exist("logs_text"):
                    dpg.add_text("Logs (app-wide)")
                    dpg.add_input_text(tag="logs_text", multiline=True, readonly=True, width=-1, height=300)
                dpg.add_spacer(height=8)

            # ---------- Settings ----------
            with dpg.tab(label="Mayhem Settings", tag="tab_settings"):
                def text_input(title, tag, value, width=-1, password=False):
                    dpg.add_text(title); dpg.add_input_text(tag=tag, default_value=value, width=width, password=password, label="##"+tag)
                    dpg.add_spacer(height=8)
                dpg.add_text("Keep Your Setup Healthy, Change Website Repo, YouTube API, IndexNow and More"); dpg.add_spacer(height=8)
                text_input("Your Website Local Repo Path", "set_repo", SET["REPO_PATH"], width=600)
                text_input("Your Website URL", "set_site", SET["SITE_URL"], width=600)
                text_input("Your YouTube Channel Handle", "set_handle", SET["CHANNEL_HANDLE"], width=600)
                text_input("Your YouTube API Key", "set_key", SET.get("YOUTUBE_API_KEY",""), password=True, width=600)
                dpg.add_text("Default Smart Refresh Days"); dpg.add_input_int(tag="set_smart", default_value=SET["SMART_REFRESH_DAYS"], width=200); dpg.add_spacer(height=8)
                dpg.add_checkbox(label="Enable IndexNow", tag="set_indexnow", default_value=SET["ENABLE_INDEXNOW"]); dpg.add_spacer(height=8)
                dpg.add_checkbox(label="Enable Google Ping", tag="set_gg_ping", default_value=SET["ENABLE_GOOGLE_PING"]); dpg.add_spacer(height=8)
                dpg.add_checkbox(label="Enable Desktop Notifications", tag="set_notify", default_value=SET["NOTIFY_ON_COMPLETE"]); dpg.add_spacer(height=8)
                text_input("Your GitHub Default Commit Message", "set_commit", SET["DEFAULT_COMMIT_MSG"])
                big_button("Save Your Settings", callback=lambda: dpg.show_item("settings_confirm"), width=200)
                dpg.add_spacer(height=8)
                dpg.add_text("", tag="settings_status")
                dpg.add_spacer(height=8)

    # ----- Modals & helpers -----
    with dpg.window(label="Edit replacement text", modal=True, show=False, tag="replace_editor", no_resize=True, no_move=True):
        dpg.add_input_text(tag="be_replace_big", multiline=True, height=420, width=720)
        with dpg.group(horizontal=True):
            dpg.add_button(
                label="Use this text",
                callback=lambda: (
                    dpg.set_value("be_replace", dpg.get_value("be_replace_big")),
                    dpg.hide_item("replace_editor")
                )
            )
        dpg.add_button(label="Cancel", callback=lambda: dpg.hide_item("replace_editor"))

    with dpg.window(label="Settings", modal=True, show=False, tag="settings_saved_modal", no_resize=True, no_move=True):
        dpg.add_text("", tag="settings_saved_text")
        dpg.add_spacer(height=8)
        dpg.add_button(label="OK", callback=lambda: dpg.hide_item("settings_saved_modal"))

    with dpg.window(label="Confirm Update", modal=True, show=False, tag="confirm_modal", no_resize=True, no_move=True):
        dpg.add_text("Run the update with the current settings?")
        dpg.add_spacer(height=8)
        dpg.add_text("", tag="confirm_details")
        with dpg.group(horizontal=True):
            big_button("Cancel", lambda: dpg.hide_item("confirm_modal"), width=200)
            big_button("Run", callback=lambda: (_confirm_and_run()), width=200)

    with dpg.window(label="First Diff", modal=True, show=False, tag="diff_modal", no_resize=True, no_move=True, width=980, height=600):
        dpg.add_input_text(tag="diff_text", multiline=True, readonly=True, width=940, height=520)

    # --- Settings confirm/save modals ---
    with dpg.window(label="Confirm Save", modal=True, show=False, tag="settings_confirm",
                no_resize=True, no_move=True, width=460, height=180):
        dpg.add_text("Save updated settings now?")
    with dpg.group(horizontal=True, parent="settings_confirm"):
        big_button("Cancel", lambda: dpg.hide_item("settings_confirm"), width=200)
        big_button("Save", lambda: (_run_thread(_save_settings_from_ui),
                                    dpg.hide_item("settings_confirm"),
                                    dpg.show_item("settings_toast")), width=200)

    with dpg.window(label="Saved", modal=False, show=False, tag="settings_toast", no_resize=True, no_move=True, no_title_bar=True, width=220, height=60):
        dpg.add_text("Settings Saved")

    with dpg.window(label="Settings Saved", modal=True, show=False, tag="settings_done_modal", no_resize=True, no_move=True):
        dpg.add_text("Settings Saved")
        dpg.add_spacer(height=8)
        big_button("OK", lambda: dpg.hide_item("settings_done_modal"), width=200)

    with dpg.window(label="About", modal=True, show=False, tag="about_modal", no_resize=True, no_move=True):
        dpg.add_text(f"{APP_NAME} — Mayhem Maker | Static site renderer & YouTube bulk tools")
        dpg.add_text("Python 3.10–3.12 | dearpygui | jinja2 | gitpython | requests")
        if _missing:
            dpg.add_spacer(height=8)
            dpg.add_text("Missing/optional dependencies:")
            for name, err in _missing:
                dpg.add_text(f"• {name}: {err}")

    # ---------- Runtime helpers & task runners ----------
    from googleapiclient.errors import HttpError

    def _is_quota_exceeded(err: Exception) -> bool:
        if not isinstance(err, HttpError):
            return False
        status = getattr(err, "resp", None).status if getattr(err, "resp", None) else None
        return status == 403 and "quotaExceeded" in str(err)

    def _quota_trip_ui(where=""):
        _be_log(f"Quota exceeded {('during ' + where) if where else ''}. Showing cached data if available.")
        try:
            be_progress(1.0, "Quota exceeded")
        except Exception:
            pass

    import threading, traceback

    def _norm_newlines(s: str) -> str:
        return (s or "").replace("\r\n", "\n").replace("\r", "\n")

    def be_apply_desc_action(desc, action, find, replace, keyword, use_regex=False, match_case=False):
        desc    = _norm_newlines(desc or "")
        find    = _norm_newlines(find or "")
        replace = _norm_newlines(replace or "")
        keyword = _norm_newlines(keyword or "")
        import re

        if action == "Find & Replace":
            if not find:
                return desc
            if use_regex:
                flags = 0 if match_case else re.IGNORECASE
                try:
                    return re.sub(find, replace, desc, flags=flags)
                except re.error:
                    return desc  # bad regex -> no change
            else:
                if match_case:
                    return desc.replace(find, replace)
                return re.sub(re.escape(find), replace, desc, flags=re.IGNORECASE)

        if action == "Prepend":
            return replace + desc

        if action == "Append":
            return desc + replace

        if action == "Replace After Keyword" and keyword and keyword in desc:
            pos = desc.find(keyword) + len(keyword)
            return desc[:pos] + replace

        if action == "Replace Before Keyword" and keyword and keyword in desc:
            pos = desc.find(keyword)
            return replace + desc[pos:]

        return desc

    def _set_quota_ui():
        try:
            dpg.set_value("quota_text_gui", f"YouTube Quota (est): Used {estimated_units} / {DAILY_QUOTA_LIMIT}")
        except Exception:
            pass

    def _append_app_log(msg: str):
        _append_log("logs_text", msg)

    def _confirm_and_run():
        dpg.hide_item("confirm_modal")
        _run_thread(_run_task_update)

    def _run_thread(fn):
        t = threading.Thread(target=lambda: _safe_wrap(fn), daemon=True)
        t.start()

    def _safe_wrap(fn):
        try:
            fn()
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            _append_log("logs_text", f"Error: {e}\n{tb}")

    def _deploy_progress(pct: float, msg: str | None):
        """Drive the bottom (deploy) progress bar safely from update_site()."""
        try:
            p = max(0.0, min(1.0, float(pct)))
        except Exception:
            p = 0.0
        try: dpg.set_value("deploy_progress_bar", p)
        except Exception: pass
        try: dpg.configure_item("deploy_progress_bar", overlay=f"{int(p*100)}%")
        except Exception: pass
        try: dpg.set_value("deploy_progress_text", str(msg or ""))
        except Exception: pass

    def _reset_progress_ui(is_dry_run: bool):
        # Reset top/outer bar (Update tab)
        try:
            dpg.set_value("progress", 0.0)
            dpg.configure_item("progress", overlay="Ready")
        except Exception:
            pass

        try:
            dpg.set_value("be_progress", 0.0)
            dpg.configure_item("be_progress", overlay="Ready")
        except Exception:
            pass

        # Reset Bulk Editor progress (if visible)
        try:
            dpg.set_value("be_progress", 0.0)
            dpg.configure_item("be_progress", overlay="0%")
            dpg.set_value("be_progress_text", "")
        except Exception:
            pass


        # Reset inner/bottom deploy bar
        try:
            dpg.configure_item("deploy_progress_wrap", show=not is_dry_run)  # hide for dry-run, show for live
            dpg.set_value("deploy_progress_bar", 0.0)
            dpg.configure_item("deploy_progress_bar", overlay="0%")
            dpg.set_value("deploy_progress_text", "")
        except Exception:
            pass

        # Reset monotonic guards so progress can move from 0 again
        try:
            be_progress._last = 0.0
        except Exception:
            pass
        try:
            deploy_progress._last = 0.0
        except Exception:
            pass

    def _first_diff_text() -> str:
        try:
            p = os.path.join(REPO_PATH, "_preview", "__last_first_diff.txt")
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return f.read()
        except Exception:
            pass
        return "No diff captured yet."

    def _summarize_update_args():
        try:
            parts = []
            parts.append("Update Index: " + ("Yes" if dpg.get_value("chk_update_index") else "No"))
            parts.append("Update Videos: " + ("Yes" if dpg.get_value("chk_update_videos") else "No"))
            parts.append("Dry-Run: " + ("Yes" if dpg.get_value("chk_dry_run") else "No"))
            parts.append("Force Rebuild: " + ("Yes" if dpg.get_value("chk_force") else "No"))
            parts.append(f'Commit: "{dpg.get_value("commit_msg")}"')
            return " • ".join(parts)
        except Exception:
            return ""

    # Wire the confirm details text when showing the modal
    def _show_confirm():
        dpg.set_value("confirm_details", _summarize_update_args())
        dpg.show_item("confirm_modal")

    # Replace the button callback to fill details (button was created earlier)
    try:
        # re-bind the "Run Update" button to show details
        # (find it by label is tricky; leave as-is if not found)
        pass
    except Exception:
        pass

    def _run_task_update():
        # Hard reset both progress bars at start
        dpg.configure_item("progress", overlay="Starting…")
        dpg.set_value("progress", 0.0)
        try:
            dpg.set_value("deploy_progress_bar", 0.0)
            dpg.configure_item("deploy_progress_bar", overlay="0%")
            dpg.set_value("deploy_progress_text", "")
        except Exception:
            pass

        # Reset UI panels
        try:
            dpg.configure_item("changed_files_list", items=[])
        except Exception:
            try: dpg.configure_item("changed_files_list", items=[...])
            except Exception: pass
        dpg.set_value("deploy_output", "")
        _append_log("logs_text", "Starting update_site…")

        # Collect args
        upd_idx   = bool(dpg.get_value("chk_update_index"))
        upd_vid   = bool(dpg.get_value("chk_update_videos"))
        dry_run   = bool(dpg.get_value("chk_dry_run"))
        force_all = bool(dpg.get_value("chk_force"))
        commit    = dpg.get_value("commit_msg") or SET["DEFAULT_COMMIT_MSG"]
        
        _reset_progress_ui(is_dry_run=dry_run)
        
        if upd_vid:
            _append_log("logs_text", f"Syncing latest (<= {SET['SMART_REFRESH_DAYS']} days) before build…")
            try:
                added, errs = fetch_latest(days=SET["SMART_REFRESH_DAYS"])
                _append_log("logs_text", f"Added {added} new videos.")
                if errs:
                    _append_log("logs_text", "Errors:\n - " + "\n - ".join(errs))
            except Exception as e:
                _append_log("logs_text", f"Fetch failed: {e}")

        # Run
        res = update_site(
            update_index=upd_idx,
            update_videos=upd_vid,
            dry_run=dry_run,
            commit_msg=commit,
            progress=be_progress,
            force_rebuild=force_all,
            deploy_ui_progress=None if dry_run else deploy_progress,
        )

        # Populate UI
        try:
            dpg.configure_item("changed_files_list", items=res.get("changed_files", []))
        except Exception:
            try: dpg.configure_item("changed_files_list", items=[])
            except Exception: pass

        deploy_out = (res.get("deploy_output") or "").strip()
        if res.get("dry_run"):
            dpg.set_value("deploy_output", "Dry run complete -> see _preview/ and Changed Files list. No deploy executed.")
        elif deploy_out:
            dpg.set_value("deploy_output", deploy_out)

        # Finalize bottom bar:
        try:
            if res.get("dry_run"):
                # keep it reset for the next run
                dpg.set_value("deploy_progress_bar", 0.0)
                dpg.configure_item("deploy_progress_bar", overlay="0%")
                dpg.set_value("deploy_progress_text", "")
            else:
                dpg.set_value("deploy_progress_bar", 1.0)
                dpg.configure_item("deploy_progress_bar", overlay="100%")
                dpg.set_value("deploy_progress_text", "Task Completed")
        except Exception:
            pass

        # Wrap up
        _set_quota_ui()
        _append_log("Update complete.")

    # ----- Cache Manager task helpers -----
    def _parse_ids(text: str) -> list[str]:
        ids = []
        if not text:
            return ids
        for tok in re.split(r"[\s,]+", text.strip()):
            if not tok:
                continue
            m = re.search(r"(?:v=|/shorts/|/watch\?v=|/live/|youtu\.be/)([A-Za-z0-9_-]{6,})", tok)
            if m:
                ids.append(m.group(1))
            elif re.fullmatch(r"[A-Za-z0-9_-]{6,}", tok):
                ids.append(tok)
        return list(dict.fromkeys(ids))  # dedupe, keep order

    def _task_remove(ids: list[str]):
        if not ids:
            _append_log("No IDs to remove.")
            return
        vids = load_cache()
        before = len(vids)
        kept = []
        removed_ids = load_removed()
        for v in vids:
            if v.get("video_id") in ids:
                removed_ids.add(v.get("video_id"))
            else:
                kept.append(v)
        save_cache(kept)
        save_removed(removed_ids)
        # Remove physical pages now
        deleted = []
        for vid in ids:
            slug = vid
            for v in vids:
                if v.get("video_id") == vid:
                    slug = output_basename(v)
                    break
            fp = os.path.join(VIDEOS_DIR, f"{slug}.html")
            if os.path.exists(fp):
                try:
                    os.remove(fp)
                    deleted.append(fp)
                except Exception:
                    pass
        _append_log(f"Removed {before - len(kept)} entries; purged {len(deleted)} files.")

    def _run_task_cache(task_fn):
        try:
            dpg.show_item("cache_busy")
            dpg.configure_item("cache_controls", show=False)
            dpg.set_value("cache_progress", 0.0)
            dpg.configure_item("cache_progress", overlay="Starting…")
            dpg.set_value("cache_progress_text", "Starting…")
        except Exception:
            pass

        stop = {"flag": False}
        def _trickle():
            val = 0.0
            while not stop["flag"] and val < 0.9:
                val = min(0.9, val + 0.03)
                try:
                    dpg.set_value("cache_progress", val)
                    dpg.configure_item("cache_progress", overlay=f"{int(val*100)}%")
                except Exception:
                    pass
                time.sleep(0.25)

        t = threading.Thread(target=_trickle, daemon=True); t.start()
        try:
            task_fn()
        finally:
            stop["flag"] = True
            try:
                dpg.hide_item("cache_busy")
                dpg.configure_item("cache_controls", show=True)
                dpg.set_value("cache_progress", 1.0)
                dpg.configure_item("cache_progress", overlay="Done")
                dpg.set_value("cache_progress_text", "Task Completed")
            except Exception:
                pass

    def _do_fetch_latest(days: int):
        added, errs = fetch_latest(days=days)
        _append_log(f"Fetch latest (<= {days} days): added {added}")
        if errs:
            _append_log("Errors:\n - " + "\n - ".join(errs))

    def _do_fetch_all():
        added, errs = fetch_all()
        _append_log(f"Fetch ALL: added {added}")
        if errs:
            _append_log("Errors:\n - " + "\n - ".join(errs))

    def _do_refresh_stale(days):
        try:
            updated, errs = refresh_stale(days=days)
            _append_log("be_log_text", f"Smart refresh: updated {updated}")
            if errs:
                for e in errs:
                    _append_log("be_log_text", f"Error: {e}")
        except QuotaExceeded as e:
            mark_quota_exhausted()
            _append_log("be_log_text", f"YouTube quota exceeded during refresh: {e}")
        except Exception as e:
            _append_log("be_log_text", f"Refresh failed: {e}")

    def _do_refresh_ids(ids: list[str]):
        if not ids:
            _append_log("No IDs parsed.")
            return
        updated, errs = refresh_stale(days=0, by_ids=set(ids))
        _append_log(f"Refreshed by IDs: {updated} (requested {len(ids)})")
        if errs:
            _append_log("Errors:\n - " + "\n - ".join(errs))

    # ----- Insights helpers -----
    def _reload_insights():
        try:
            vids = load_cache() or []
            if not vids:
                for t in ("ins_count","ins_views","ins_avgdur","ins_likes","ins_comments","ins_meanviews","ins_medianviews"):
                    try: dpg.set_value(t, "0")
                    except: pass
                # also clear plots if present
                for ax in ("plot_views_y","plot_top20_y","plot_bydate_y","plot_months_y"):
                    if dpg.does_item_exist(ax):
                        for c in dpg.get_item_children(ax, 1) or []:
                            dpg.delete_item(c)
                return

            def _int(x):
                try: return int(x)
                except: return 0

            # flatten fields
            views     = [_int(v.get("view_count") or v.get("statistics",{}).get("viewCount")) for v in vids]
            likes     = [_int(v.get("like_count")  or v.get("statistics",{}).get("likeCount"))  for v in vids]
            comments  = [_int(v.get("comment_count") or v.get("statistics",{}).get("commentCount")) for v in vids]
            durations = [_int(v.get("duration_seconds") or 0) for v in vids]

            import statistics
            total_views    = sum(views)
            avg_dur        = round(statistics.mean(durations), 1) if durations else 0
            total_likes    = sum(likes)
            total_comments = sum(comments)
            mean_views     = round(statistics.mean(views), 1) if views else 0
            median_views   = int(statistics.median(views)) if views else 0

            # header counters
            dpg.set_value("ins_count",       str(len(vids)))
            dpg.set_value("ins_views",       f"{total_views:,}")
            dpg.set_value("ins_avgdur",      str(avg_dur))
            dpg.set_value("ins_likes",       f"{total_likes:,}")
            dpg.set_value("ins_comments",    f"{total_comments:,}")
            dpg.set_value("ins_meanviews",   f"{mean_views:,}")
            dpg.set_value("ins_medianviews", f"{median_views:,}")

            # === Plot 1: Views (descending)
            if dpg.does_item_exist("plot_views_y"):
                for c in dpg.get_item_children("plot_views_y", 1) or []:
                    dpg.delete_item(c)
                ys = sorted(views, reverse=True)
                xs = list(range(1, len(ys)+1))
                dpg.add_line_series(xs, ys, label="Views", parent="plot_views_y")

            # === Plot 2: Top 20 by Views (bar)
            if dpg.does_item_exist("plot_top20_y"):
                for c in dpg.get_item_children("plot_top20_y", 1) or []:
                    dpg.delete_item(c)
                top = sorted(views, reverse=True)[:20]
                xs  = list(range(1, len(top)+1))
                dpg.add_bar_series(xs, top, weight=0.7, label="Views", parent="plot_top20_y")

            # === Plot 3: Views by Publish Date (chronological line)
            if dpg.does_item_exist("plot_bydate_y"):
                for c in dpg.get_item_children("plot_bydate_y", 1) or []:
                    dpg.delete_item(c)
                # assume vids already chronological oldest->newest if your cache preserves this;
                # else sort by a timestamp field if present.
                ys = views[:]  # same order as cache
                xs = list(range(1, len(ys)+1))
                dpg.add_line_series(xs, ys, label="Views", parent="plot_bydate_y")

            # === Plot 4: Uploads by Month (bar)
            if dpg.does_item_exist("plot_months_y"):
                for c in dpg.get_item_children("plot_months_y", 1) or []:
                    dpg.delete_item(c)

                from collections import Counter
                def month_key(v):
                    # try a few fields for published date
                    s = (v.get("published_at") or v.get("snippet",{}).get("publishedAt") or "")[:7]  # YYYY-MM
                    return s if len(s) == 7 else "unknown"
                counts = Counter(month_key(v) for v in vids if month_key(v) != "unknown")
                if counts:
                    months = sorted(counts.keys())
                    xs = list(range(len(months)))
                    ys = [counts[m] for m in months]
                    dpg.add_bar_series(xs, ys, weight=0.7, label="# Uploads", parent="plot_months_y")
                    # show nice tick labels on X axis if you want
                    try:
                        dpg.set_axis_ticks("plot_months_x", [(i, m) for i, m in enumerate(months)])
                    except Exception:
                        pass

        except Exception as e:
            _append_log("logs_text", f"Insights reload failed: {e}")

    def _save_settings_from_ui():
        SET["REPO_PATH"]         = dpg.get_value("set_repo") or SET["REPO_PATH"]
        SET["SITE_URL"]          = (dpg.get_value("set_site") or SET["SITE_URL"]).rstrip("/")
        SET["CHANNEL_HANDLE"]    = dpg.get_value("set_handle") or SET["CHANNEL_HANDLE"]
        SET["YOUTUBE_API_KEY"]   = dpg.get_value("set_key") or SET.get("YOUTUBE_API_KEY", "")
        SET["SMART_REFRESH_DAYS"]= int(dpg.get_value("set_smart") or SET["SMART_REFRESH_DAYS"])
        SET["ENABLE_INDEXNOW"]   = bool(dpg.get_value("set_indexnow"))
        SET["ENABLE_GOOGLE_PING"]= bool(dpg.get_value("set_gg_ping"))
        SET["NOTIFY_ON_COMPLETE"]= bool(dpg.get_value("set_notify"))
        SET["DEFAULT_COMMIT_MSG"]= dpg.get_value("set_commit") or SET["DEFAULT_COMMIT_MSG"]

        save_settings(SET)
        _append_log("Settings saved. Some changes may require restarting the app.")
        try:
            dpg.set_value("settings_saved_text", "✅ Settings saved.")
            dpg.show_item("settings_saved_modal")
        except Exception:
            pass

        try:
            dpg.set_value("settings_status", f"✓ Settings saved at {utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:
            pass

    def _first_diff_text_action():
        dpg.set_value("diff_text", _first_diff_text())

    # Seed UI state
    _set_quota_ui()
    try:
        # Hook the "Run Update" button to show details if it exists
        # (We can't easily grab the exact id created earlier, so we leave the button as-is.)
        pass
    except Exception:
        pass
    
    # ------- Finish GUI lifecycle (correct order + safe font bind + maximize) -------
    dpg.setup_dearpygui()
    dpg.show_viewport()

    # Make 'root' the primary window and fit it to the viewport
    try:
        dpg.set_primary_window("root", True)
    except Exception:
        pass
    try:
        _fit_root_to_viewport()
        # Keep it fitted as the user resizes the app window
        dpg.set_viewport_resize_callback(lambda sender, app_data: _fit_root_to_viewport())
    except Exception:
        pass
    # Optional: open maximized
    try:
        maximize_on_first_run()
    except Exception:
        pass
    if isinstance(fonts, dict) and fonts.get("ui"):
        dpg.bind_font(fonts["ui"])
    apply_theme(SET.get("THEME", "auto"))
    dpg.start_dearpygui()

# ===================== ARGPARSE / ENTRYPOINT =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="mayhem_maker",
        description="Render site, fetch/refresh cache, or open the GUI."
    )
    # RENDER / DEPLOY
    parser.add_argument("--update-index",  action="store_true")
    parser.add_argument("--update-videos", action="store_true")
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--commit-msg",    type=str, default=None)
    parser.add_argument("--force-rebuild", action="store_true")
    # CACHE / API
    parser.add_argument("--fetch-latest",  type=int, metavar="DAYS")
    parser.add_argument("--fetch-all",     action="store_true")
    parser.add_argument("--refresh-stale", type=int, metavar="DAYS")
    parser.add_argument("--reslug-title",          action="store_true")
    parser.add_argument("--reslug-title-oembed",   action="store_true")
    # GUI toggle (optional)
    parser.add_argument("--gui", action="store_true")

    args = parser.parse_args()

    # Simple, predictable behavior:
    # - If you passed ANY CLI action flags -> CLI mode
    # - Otherwise (no flags) OR --gui -> GUI mode
    cli_actions = any([
        args.update_index, args.update_videos, args.dry_run,
        args.commit_msg is not None, args.force_rebuild,
        args.fetch_latest is not None, args.fetch_all,
        args.refresh_stale is not None, args.reslug_title, args.reslug_title_oembed
    ])

    if args.gui or not cli_actions:
        try:
            start_gui()
        except Exception:
            import traceback; traceback.print_exc()
            input("GUI crashed. Press Enter to exit.")
    else:
        run_cli(args)
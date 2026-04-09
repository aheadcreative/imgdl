#!/usr/bin/env python3
"""Image downloader CLI — fetch, resize, and save images matching text descriptions."""

import argparse
import csv
import io
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml
from PIL import Image
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Domain map for Clearbit logo lookups
# ---------------------------------------------------------------------------

DOMAIN_MAP = {
    "openai": "openai.com",
    "anthropic": "anthropic.com",
    "google deepmind": "deepmind.google",
    "meta ai": "meta.com",
    "mistral ai": "mistral.ai",
    "cohere": "cohere.com",
    "stability ai": "stability.ai",
    "hugging face": "huggingface.co",
    "xai": "x.ai",
    "perplexity": "perplexity.ai",
    "inflection ai": "inflection.ai",
    "ai21 labs": "ai21.com",
    "aleph alpha": "aleph-alpha.com",
    "databricks": "databricks.com",
    "jasper ai": "jasper.ai",
    "copy.ai": "copy.ai",
    "notion ai": "notion.so",
    "grammarly": "grammarly.com",
    "midjourney": "midjourney.com",
    "runway": "runwayml.com",
    "descript": "descript.com",
    "synthesia": "synthesia.io",
    "heygen": "heygen.com",
    "tome": "tome.app",
    "beautiful.ai": "beautiful.ai",
    "gamma": "gamma.app",
    "otter.ai": "otter.ai",
    "fireflies.ai": "fireflies.ai",
    "harvey ai": "harvey.ai",
    "casetext": "casetext.com",
    "glean": "glean.com",
    "writer": "writer.com",
    "typeface": "typeface.ai",
    "adobe firefly": "adobe.com",
    "canva ai": "canva.com",
    "figma ai": "figma.com",
    "github copilot": "github.com",
    "cursor": "cursor.com",
    "replit": "replit.com",
    "vercel v0": "vercel.com",
    "google": "google.com",
    "microsoft": "microsoft.com",
    "apple": "apple.com",
    "amazon": "amazon.com",
    "tesla": "tesla.com",
    "nvidia": "nvidia.com",
    "slack": "slack.com",
    "stripe": "stripe.com",
    "shopify": "shopify.com",
    "spotify": "spotify.com",
    "discord": "discord.com",
    "twitch": "twitch.tv",
    "reddit": "reddit.com",
    "twitter": "x.com",
    "x": "x.com",
    "linkedin": "linkedin.com",
    "pinterest": "pinterest.com",
    "airbnb": "airbnb.com",
    "uber": "uber.com",
    "lyft": "lyft.com",
    "dropbox": "dropbox.com",
    "zoom": "zoom.us",
    "salesforce": "salesforce.com",
    "oracle": "oracle.com",
    "ibm": "ibm.com",
    "intel": "intel.com",
    "amd": "amd.com",
    "samsung": "samsung.com",
    "sony": "sony.com",
    "netflix": "netflix.com",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

URL_RE = re.compile(r"^https?://", re.IGNORECASE)

# Patterns for cleaning messy pasted lists
_LIST_PREFIX_RE = re.compile(r"^\s*(?:\d+[\.\)]\s*)?(?:[-*]\s*)?")
_BRACKET_REF_RE = re.compile(r"\[\d+\]")
_URL_INLINE_RE = re.compile(r"https?://\S+")
_SEPARATOR_RE = re.compile(r"\s*[—–\-|:]\s*")

# Recognised column names (lowercase) → QueryItem field
COLUMN_ALIASES = {
    "query": "query", "name": "query", "search": "query", "description": "query",
    "url": "url", "image_url": "url", "link": "url", "src": "url",
    "size": "size",
    "type": "type", "category": "type",
    "background": "background", "bg": "background",
    "format": "format", "fmt": "format", "ext": "format",
    "filename": "filename", "file": "filename", "output_name": "filename",
}


@dataclass
class QueryItem:
    """One download job. Fields default to None → inherit from CLI args."""
    query: str | None = None
    url: str | None = None
    size: tuple[int, int] | None = None
    type: str | None = None
    background: str | None = None
    format: str | None = None
    filename: str | None = None


def parse_pasted_text(text: str) -> list[QueryItem]:
    """Parse a messy pasted list into clean QueryItems.
    Handles numbered lists, bullet points, inline URLs, reference brackets, em dashes."""
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Strip reference brackets like [4] [5][8]
        line = _BRACKET_REF_RE.sub("", line)

        # Extract and remove inline URLs
        line = _URL_INLINE_RE.sub("", line)

        # Strip list prefixes (1. / 2) / - / * )
        line = _LIST_PREFIX_RE.sub("", line)

        # Split on separator (em dash, pipe, colon) and take the first part as the name
        parts = _SEPARATOR_RE.split(line)
        name = parts[0].strip().rstrip(".,;") if parts else line.strip()

        # Remove trailing parenthetical like "(GPT-4)" or "(Lyro)"
        name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()

        if not name:
            continue

        items.append(QueryItem(query=name))
    return items


def sanitise_filename(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def parse_size(size_str: str) -> tuple[int, int]:
    m = re.match(r"^(\d+)x(\d+)$", size_str.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid size format: {size_str} (expected WxH, e.g. 300x300)")
    return int(m.group(1)), int(m.group(2))


def _row_to_item(row: dict[str, str]) -> QueryItem | None:
    """Convert a column-mapped dict to a QueryItem."""
    item = QueryItem()
    for raw_col, value in row.items():
        value = value.strip() if value else ""
        if not value:
            continue
        mapped = COLUMN_ALIASES.get(raw_col.lower().strip())
        if mapped == "query":
            item.query = value
        elif mapped == "url":
            item.url = value
        elif mapped == "size":
            try:
                item.size = parse_size(value)
            except argparse.ArgumentTypeError:
                pass
        elif mapped == "type":
            item.type = value
        elif mapped == "background":
            item.background = value
        elif mapped == "format":
            item.format = value.lower()
        elif mapped == "filename":
            item.filename = value

    # Auto-detect URL in query column
    if item.query and not item.url and URL_RE.match(item.query):
        item.url = item.query
        item.query = None

    # If no query but has url, derive a name from the URL
    if not item.query and item.url:
        item.query = Path(item.url.split("?")[0].split("#")[0]).stem or "image"
    if not item.query and not item.url:
        return None
    return item


def _load_csv(path: Path) -> list[QueryItem]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        sniffer = csv.Sniffer()
        has_header = False
        try:
            has_header = sniffer.has_header(sample)
        except csv.Error:
            pass

        if has_header:
            reader = csv.DictReader(f)
            items = []
            for row in reader:
                item = _row_to_item(row)
                if item:
                    items.append(item)
            return items
        else:
            reader = csv.reader(f)
            items = []
            for row in reader:
                if not row or not row[0].strip():
                    continue
                val = row[0].strip()
                if URL_RE.match(val):
                    items.append(QueryItem(url=val, query=Path(val.split("?")[0]).stem or "image"))
                else:
                    items.append(QueryItem(query=val))
            return items


def _load_xlsx(path: Path) -> list[QueryItem]:
    try:
        import openpyxl
    except ImportError:
        print("ERROR: openpyxl is required for .xlsx files: pip install openpyxl", file=sys.stderr)
        sys.exit(1)

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []

    # Detect header row
    first_row = [str(c).strip().lower() if c else "" for c in rows[0]]
    has_header = any(c in COLUMN_ALIASES for c in first_row)

    if has_header:
        headers = [str(c).strip() if c else f"col{i}" for i, c in enumerate(rows[0])]
        items = []
        for row in rows[1:]:
            d = {headers[i]: str(cell) if cell is not None else "" for i, cell in enumerate(row)}
            item = _row_to_item(d)
            if item:
                items.append(item)
        return items
    else:
        items = []
        for row in rows:
            val = str(row[0]).strip() if row[0] else ""
            if not val:
                continue
            if URL_RE.match(val):
                items.append(QueryItem(url=val, query=Path(val.split("?")[0]).stem or "image"))
            else:
                items.append(QueryItem(query=val))
        return items


def load_queries(query_arg: str) -> list[QueryItem]:
    """Load queries from a string, URL, .txt, .csv, or .xlsx file."""

    # Direct URL passed on CLI
    if URL_RE.match(query_arg):
        name = Path(query_arg.split("?")[0].split("#")[0]).stem or "image"
        return [QueryItem(query=name, url=query_arg)]

    path = Path(query_arg)
    if path.is_file():
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return _load_csv(path)
        elif suffix in (".xlsx", ".xls"):
            return _load_xlsx(path)
        else:
            # Plain text file — one query or URL per line
            items = []
            for line in path.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                if URL_RE.match(line):
                    name = Path(line.split("?")[0].split("#")[0]).stem or "image"
                    items.append(QueryItem(query=name, url=line))
                else:
                    items.append(QueryItem(query=line))
            return items

    # Plain query string
    return [QueryItem(query=query_arg)]


def load_config() -> dict:
    for name in (".imgdl.yaml", ".imgdl.yml"):
        p = Path(name)
        if p.is_file():
            with open(p) as f:
                return yaml.safe_load(f) or {}
    return {}


WINE_TYPES = {"bottle", "wine"}
PRODUCT_TYPES = {"product", "product photo"}


def build_search_query(query: str, img_type: str | None, background: str | None) -> str:
    parts = [query]
    if img_type in WINE_TYPES:
        parts.append("wine bottle front label high resolution")
    elif img_type in PRODUCT_TYPES:
        parts.append("product photo high resolution")
    elif img_type:
        parts.append(img_type)
    if background and background != "none":
        parts.append(f"{background} background")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Image sources
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> bytes | None:
    """Directly download an image from a URL (skip search)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        r = requests.get(url, timeout=15, headers=headers)
        if r.status_code == 200 and len(r.content) > 100:
            return r.content
    except requests.RequestException:
        pass
    return None


def fetch_google_favicon(domain: str) -> bytes | None:
    """Fetch high-res favicon via Google's S2 service (Clearbit replacement)."""
    url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 100:
            return r.content
    except requests.RequestException:
        pass
    return None


_VIVINO_IMG_HASH_RE = re.compile(r"images\.vivino\.com/thumbs/([a-zA-Z0-9_-]+)_p[bl]_[\dx]+\.png")
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def fetch_vivino(query: str, count: int) -> list[bytes]:
    """Search Vivino and download high-res bottle images (960px tall)."""
    try:
        r = requests.get(
            "https://www.vivino.com/search/wines",
            params={"q": query},
            headers={"User-Agent": _BROWSER_UA},
            timeout=15,
        )
        if r.status_code != 200:
            return []
    except requests.RequestException:
        return []

    # Extract unique image hashes from search page
    from collections import OrderedDict
    hashes = list(OrderedDict.fromkeys(_VIVINO_IMG_HASH_RE.findall(r.text)))
    if not hashes:
        return []

    results = []
    for h in hashes[: count + 2]:
        if len(results) >= count:
            break
        url = f"https://images.vivino.com/thumbs/{h}_pb_x960.png"
        try:
            img_r = requests.get(url, headers={"User-Agent": _BROWSER_UA}, timeout=10)
            if img_r.status_code == 200 and len(img_r.content) > 500:
                results.append(img_r.content)
        except requests.RequestException:
            continue
    return results


def fetch_wine_searcher(query: str, count: int) -> list[bytes]:
    """Search DuckDuckGo for wine images scoped to wine-searcher.com."""
    return fetch_duckduckgo(f"{query} wine bottle site:wine-searcher.com", count)


def fetch_google_cse(query: str, count: int, api_key: str, cse_id: str,
                     img_type: str | None) -> list[bytes]:
    params = {
        "q": query,
        "cx": cse_id,
        "key": api_key,
        "searchType": "image",
        "num": min(count, 10),
    }
    if img_type == "logo":
        params["imgType"] = "clipart"
    elif img_type in ("icon",):
        params["imgType"] = "clipart"
    elif img_type == "headshot":
        params["imgType"] = "face"

    try:
        r = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=15)
        if r.status_code != 200:
            return []
        items = r.json().get("items", [])
    except (requests.RequestException, ValueError):
        return []

    results = []
    for item in items[:count]:
        link = item.get("link", "")
        try:
            img_r = requests.get(link, timeout=10, headers={"User-Agent": "imgdl/1.0"})
            if img_r.status_code == 200 and len(img_r.content) > 100:
                results.append(img_r.content)
        except requests.RequestException:
            continue
    return results


def fetch_duckduckgo(query: str, count: int) -> list[bytes]:
    try:
        from ddgs import DDGS
    except ImportError:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            return []

    results = []
    try:
        with DDGS() as ddgs:
            images = list(ddgs.images(query, max_results=count + 3))
    except Exception:
        return []

    for img in images[:count + 3]:
        if len(results) >= count:
            break
        url = img.get("image", "")
        if not url:
            continue
        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "imgdl/1.0"})
            if r.status_code == 200 and len(r.content) > 200:
                results.append(r.content)
        except requests.RequestException:
            continue
    return results


def fetch_brave(query: str, count: int, api_key: str) -> list[bytes]:
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/images/search",
            params={"q": query, "count": min(count + 2, 10)},
            headers={"Accept": "application/json", "Accept-Encoding": "gzip", "X-Subscription-Token": api_key},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("results", [])
    except (requests.RequestException, ValueError):
        return []

    results = []
    for item in items:
        if len(results) >= count:
            break
        url = item.get("properties", {}).get("url") or item.get("thumbnail", {}).get("src", "")
        if not url:
            continue
        try:
            img_r = requests.get(url, timeout=10, headers={"User-Agent": "imgdl/1.0"})
            if img_r.status_code == 200 and len(img_r.content) > 200:
                results.append(img_r.content)
        except requests.RequestException:
            continue
    return results


# ---------------------------------------------------------------------------
# Image processing
# ---------------------------------------------------------------------------

def process_image(
    raw: bytes,
    target_w: int,
    target_h: int,
    padding_pct: int,
    bg: str,
    fmt: str,
    transparent_only: bool,
    min_source_pct: int = 0,
) -> bytes | None:
    try:
        img = Image.open(io.BytesIO(raw))
    except Exception:
        return None

    # Reject source images that are too small (would produce blurry upscales)
    if min_source_pct > 0:
        threshold = min_source_pct / 100
        src_w, src_h = img.size
        if src_w < target_w * threshold and src_h < target_h * threshold:
            return None

    if img.mode not in ("RGBA", "LA", "PA"):
        img = img.convert("RGBA")

    if transparent_only:
        if img.mode != "RGBA" or img.getextrema()[3][0] == 255:
            return None

    # Calculate inner box after padding
    if padding_pct > 0:
        scale = (100 - padding_pct) / 100
        inner_w = int(target_w * scale)
        inner_h = int(target_h * scale)
    else:
        inner_w, inner_h = target_w, target_h

    # Resize to fit within inner box, maintaining aspect ratio
    img.thumbnail((inner_w, inner_h), Image.LANCZOS)

    # Determine background
    use_alpha = fmt == "png" and bg in ("transparent", "none")
    if use_alpha:
        canvas = Image.new("RGBA", (target_w, target_h), (0, 0, 0, 0))
    else:
        bg_color = (255, 255, 255) if bg in ("white", "transparent", "none") else (30, 30, 30)
        canvas = Image.new("RGBA", (target_w, target_h), (*bg_color, 255))

    # Centre paste
    x = (target_w - img.width) // 2
    y = (target_h - img.height) // 2
    canvas.paste(img, (x, y), img if img.mode == "RGBA" else None)

    # Convert for output format
    buf = io.BytesIO()
    if fmt == "jpg":
        out = canvas.convert("RGB")
        out.save(buf, format="JPEG", quality=90)
    elif fmt == "webp":
        canvas.save(buf, format="WEBP", quality=90)
    else:
        canvas.save(buf, format="PNG")

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main download logic
# ---------------------------------------------------------------------------

@dataclass
class DownloadOpts:
    """Download options — populated from CLI args or web form."""
    size: tuple[int, int] = (300, 300)
    type: str | None = None
    background: str | None = None
    format: str = "png"
    count: int = 1
    padding: int = 0
    output: str = "./downloads"
    dry_run: bool = False
    transparent_only: bool = False
    overwrite: bool = False
    skip_existing: bool = False
    min_source_pct: int = 0  # 0 = accept any size, 50 = source must be ≥50% of target


def download_images_for_query(
    item: QueryItem,
    opts: DownloadOpts,
    google_key: str | None,
    google_id: str | None,
    brave_key: str | None,
    output_dir: Path,
    failed_log,
) -> dict:
    stats = {"downloaded": 0, "failed": 0, "skipped": 0, "files": []}

    # Per-item overrides (from multi-column CSV/XLSX) fall back to opts
    target_w, target_h = item.size or opts.size
    img_type = item.type or opts.type
    background = item.background or opts.background
    fmt = item.format or opts.format
    query = item.query or "image"
    base_name = sanitise_filename(item.filename or query)

    search_q = build_search_query(query, img_type, background)

    # Check skip-existing
    if opts.skip_existing:
        existing = list(output_dir.glob(f"{base_name}_*.{fmt}"))
        if len(existing) >= opts.count:
            stats["skipped"] = opts.count
            return stats

    if opts.dry_run:
        for i in range(1, opts.count + 1):
            fname = f"{base_name}_{i}.{fmt}"
            tqdm.write(f"  [dry-run] Would save: {output_dir / fname}")
        stats["downloaded"] = opts.count
        return stats

    raw_images: list[bytes] = []

    # Direct URL mode — skip search entirely
    if item.url:
        data = fetch_url(item.url)
        if data:
            raw_images.append(data)
        else:
            failed_log.write(f"{query}\tFailed to download URL: {item.url}\n")
            stats["failed"] = 1
            return stats
    else:
        is_wine = img_type in WINE_TYPES

        # Source 1a: Vivino (wine/bottle types)
        if is_wine and len(raw_images) < opts.count:
            needed = opts.count - len(raw_images)
            raw_images.extend(fetch_vivino(query, needed))
            time.sleep(0.5)

        # Source 1b: Google Favicon (logo type only, known domains)
        if img_type == "logo" and not raw_images:
            key = query.lower().strip()
            domain = DOMAIN_MAP.get(key)
            if domain:
                data = fetch_google_favicon(domain)
                if data:
                    raw_images.append(data)
                time.sleep(0.3)

        # Source 2: Google CSE
        if len(raw_images) < opts.count and google_key and google_id:
            needed = opts.count - len(raw_images)
            raw_images.extend(fetch_google_cse(search_q, needed, google_key, google_id, img_type))
            time.sleep(1)

        # Source 3: DuckDuckGo
        if len(raw_images) < opts.count:
            needed = opts.count - len(raw_images)
            raw_images.extend(fetch_duckduckgo(search_q, needed))
            time.sleep(1)

        # Source 3b: Wine-Searcher via DDG (wine fallback)
        if is_wine and len(raw_images) < opts.count:
            needed = opts.count - len(raw_images)
            raw_images.extend(fetch_wine_searcher(query, needed))
            time.sleep(1)

        # Source 4: Brave
        if len(raw_images) < opts.count and brave_key:
            needed = opts.count - len(raw_images)
            raw_images.extend(fetch_brave(search_q, needed, brave_key))
            time.sleep(1)

    if not raw_images:
        failed_log.write(f"{query}\tNo images found from any source\n")
        stats["failed"] = opts.count
        return stats

    saved = 0
    for i, raw in enumerate(raw_images[: opts.count], start=1):
        fname = f"{base_name}_{i}.{fmt}"
        fpath = output_dir / fname

        if opts.skip_existing and fpath.exists():
            stats["skipped"] += 1
            continue

        processed = process_image(
            raw, target_w, target_h, opts.padding, background or "white",
            fmt, opts.transparent_only, opts.min_source_pct,
        )
        if processed is None:
            failed_log.write(f"{query}\tImage too small or processing failed (index {i})\n")
            stats["failed"] += 1
            continue

        fpath.write_bytes(processed)
        stats["files"].append(fname)
        saved += 1

    stats["downloaded"] = saved
    if saved < opts.count:
        stats["failed"] += opts.count - saved - stats["skipped"]

    return stats


def run_batch(items: list[QueryItem], opts: DownloadOpts,
              google_key=None, google_id=None, brave_key=None):
    """Run a batch download, yielding progress dicts for each item.
    Used by both CLI (via main) and web server."""
    output_dir = Path(opts.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    failed_log_path = output_dir / "failed.log"

    total = {"downloaded": 0, "failed": 0, "skipped": 0, "files": []}
    with open(failed_log_path, "w") as failed_log:
        for idx, item in enumerate(items):
            stats = download_images_for_query(
                item, opts, google_key, google_id, brave_key, output_dir, failed_log,
            )
            total["downloaded"] += stats["downloaded"]
            total["failed"] += stats["failed"]
            total["skipped"] += stats["skipped"]
            total["files"].extend(stats.get("files", []))
            yield {
                "current": idx + 1,
                "total": len(items),
                "query": item.query or "",
                "stats": stats,
                "cumulative": {k: v for k, v in total.items() if k != "files"},
            }

    if failed_log_path.exists() and failed_log_path.stat().st_size == 0:
        failed_log_path.unlink()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    cfg = load_config()

    parser = argparse.ArgumentParser(
        description="Download, resize, and save images matching text descriptions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               '  python imgdl.py --query "Anthropic logo" --size 300x300 --type logo\n'
               '  python imgdl.py --query queries.txt --size 300x300 --type logo\n'
               '  python imgdl.py --query "https://example.com/logo.png" --size 200x200\n'
               '  python imgdl.py --query logos.xlsx --size 300x300\n'
               "\n"
               "Input formats:\n"
               "  String     Single search query or image URL\n"
               "  .txt       One query or URL per line\n"
               "  .csv       Single column (queries/URLs) or multi-column with headers:\n"
               "             query, url, size, type, background, format, filename\n"
               "  .xlsx      Same column support as CSV\n",
    )
    parser.add_argument("--query", required=True,
                        help="Search query, image URL, or path to .txt/.csv/.xlsx file")
    parser.add_argument("--size", default=cfg.get("size", "300x300"), help="Output WxH (default: 300x300)")
    parser.add_argument("--type", default=cfg.get("type"), help="Category hint: logo, label, icon, headshot, etc.")
    parser.add_argument("--background", default=cfg.get("background"), help="Background keyword: transparent, white, dark, none")
    parser.add_argument("--format", default=cfg.get("format", "png"), choices=["png", "jpg", "webp"], help="Output format (default: png)")
    parser.add_argument("--output", default=cfg.get("output", "./downloads"), help="Output directory (default: ./downloads)")
    parser.add_argument("--count", type=int, default=cfg.get("count", 1), help="Images per query (default: 1, max 5)")
    parser.add_argument("--padding", type=int, default=cfg.get("padding", 0), help="Padding %% when resizing (default: 0)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be downloaded without fetching")
    parser.add_argument("--transparent-only", action="store_true", help="Keep only images with alpha transparency")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--skip-existing", action="store_true", help="Skip queries whose output files already exist")
    parser.add_argument("--min-source-pct", type=int, default=None,
                        help="Minimum source image size as %% of target (default: 50 for wine/bottle, 0 otherwise)")

    args = parser.parse_args()

    # Auto-defaults for wine/bottle/product types
    img_type = args.type
    min_src = args.min_source_pct
    if min_src is None:
        min_src = 50 if img_type in WINE_TYPES | PRODUCT_TYPES else 0

    opts = DownloadOpts(
        size=parse_size(args.size),
        type=img_type,
        background=args.background,
        format=args.format,
        count=max(1, min(args.count, 5)),
        padding=args.padding,
        output=args.output,
        dry_run=args.dry_run,
        transparent_only=args.transparent_only,
        overwrite=args.overwrite,
        skip_existing=args.skip_existing,
        min_source_pct=min_src,
    )

    # API keys from config or env
    sources_cfg = cfg.get("sources", {})
    google_key = os.environ.get("GOOGLE_CSE_API_KEY") or sources_cfg.get("google_cse_key")
    google_id = os.environ.get("GOOGLE_CSE_ID") or sources_cfg.get("google_cse_id")
    brave_key = os.environ.get("BRAVE_API_KEY") or sources_cfg.get("brave_api_key")

    items = load_queries(args.query)

    has_urls = any(i.url for i in items)
    print(f"Processing {len(items)} {'query' if len(items) == 1 else 'queries'} → {opts.output}/")
    if opts.dry_run:
        print("[DRY RUN MODE]")

    sources_available = []
    if has_urls:
        sources_available.append("Direct URL")
    if opts.type in WINE_TYPES:
        sources_available.append("Vivino")
    if opts.type == "logo":
        sources_available.append("Google Favicon")
    if google_key and google_id:
        sources_available.append("Google CSE")
    sources_available.append("DuckDuckGo")
    if brave_key:
        sources_available.append("Brave")
    print(f"Sources: {' → '.join(sources_available)}")
    print()

    total = {"downloaded": 0, "failed": 0, "skipped": 0}
    with tqdm(total=len(items), desc="Downloading", unit="query") as pbar:
        for progress in run_batch(items, opts, google_key, google_id, brave_key):
            total = progress["cumulative"]
            pbar.update(1)

    print()
    print(f"Downloaded: {total['downloaded']} | Failed: {total['failed']} | Skipped: {total['skipped']}")


if __name__ == "__main__":
    main()

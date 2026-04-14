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

# Domain map for Google Favicon logo lookups
DOMAIN_MAP = {
    "openai": "openai.com", "anthropic": "anthropic.com",
    "google deepmind": "deepmind.google", "meta ai": "meta.com",
    "mistral ai": "mistral.ai", "cohere": "cohere.com",
    "stability ai": "stability.ai", "hugging face": "huggingface.co",
    "xai": "x.ai", "perplexity": "perplexity.ai",
    "inflection ai": "inflection.ai", "ai21 labs": "ai21.com",
    "aleph alpha": "aleph-alpha.com", "databricks": "databricks.com",
    "jasper ai": "jasper.ai", "copy.ai": "copy.ai",
    "notion ai": "notion.so", "grammarly": "grammarly.com",
    "midjourney": "midjourney.com", "runway": "runwayml.com",
    "descript": "descript.com", "synthesia": "synthesia.io",
    "heygen": "heygen.com", "tome": "tome.app",
    "beautiful.ai": "beautiful.ai", "gamma": "gamma.app",
    "otter.ai": "otter.ai", "fireflies.ai": "fireflies.ai",
    "harvey ai": "harvey.ai", "casetext": "casetext.com",
    "glean": "glean.com", "writer": "writer.com",
    "typeface": "typeface.ai", "adobe firefly": "adobe.com",
    "canva ai": "canva.com", "figma ai": "figma.com",
    "github copilot": "github.com", "cursor": "cursor.com",
    "replit": "replit.com", "vercel v0": "vercel.com",
    "google": "google.com", "microsoft": "microsoft.com",
    "apple": "apple.com", "amazon": "amazon.com",
    "tesla": "tesla.com", "nvidia": "nvidia.com",
    "slack": "slack.com", "stripe": "stripe.com",
    "shopify": "shopify.com", "spotify": "spotify.com",
    "discord": "discord.com", "netflix": "netflix.com",
}

# Italian and international wine e-commerce sites with good product photography.
# Vivino is first because its product shots and name-matching are the most reliable.
WINE_ECOMMERCE_SITES = [
    "vivino.com",
    "tannico.it", "callmewine.com", "xtrawine.com", "vinicum.it",
    "bernabei.it", "vino75.com", "giordanovini.it", "wine.com",
    "wine-searcher.com", "winehouse.it", "enotecaitaliana.it",
    "svinando.com", "wineowine.it",
]

# Source quality tiers — higher = more trusted product photography
SOURCE_QUALITY = {
    "vivino.com": 100,
    "tannico.it": 95, "callmewine.com": 85, "xtrawine.com": 85,
    "vinicum.it": 80, "bernabei.it": 80, "vino75.com": 80,
    "giordanovini.it": 75, "wine.com": 85, "wine-searcher.com": 75,
    "winehouse.it": 75, "enotecaitaliana.it": 75, "svinando.com": 75,
    "wineowine.it": 70,
    "google_cse": 60, "duckduckgo": 40, "brave": 40,
    "direct_url": 80, "favicon": 50,
}


def build_search_query(query: str, img_type: str | None, background: str | None) -> str:
    parts = [query]
    if img_type in WINE_TYPES:
        parts.append("wine bottle")
    elif img_type in PRODUCT_TYPES:
        parts.append("product photo")
    elif img_type:
        parts.append(img_type)
    if background and background != "none" and img_type not in WINE_TYPES | PRODUCT_TYPES:
        parts.append(f"{background} background")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Candidate-based image sourcing
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    """An image candidate found during search — scored and ranked before download."""
    url: str
    source: str  # e.g. "tannico.it", "wine.com", "duckduckgo"
    width: int = 0
    height: int = 0
    score: float = 0.0
    title: str = ""  # page title / alt text from search result (used for relevance)


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _ddgs_image_search(query: str, max_results: int = 8) -> list[dict]:
    """Run a DuckDuckGo image search returning raw result dicts."""
    try:
        from ddgs import DDGS
    except ImportError:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            return []
    try:
        with DDGS() as ddgs:
            return list(ddgs.images(query, max_results=max_results))
    except Exception:
        return []


def _source_from_url(url: str) -> str:
    """Extract a source key from a URL for quality scoring. Matches domain suffix only."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        # Strip common CDN prefixes so "images.tannico.it" → "tannico.it"
        for prefix in ("www.", "images.", "img.", "cdn.", "static.", "assets.", "s."):
            host = host.removeprefix(prefix)
        for site in WINE_ECOMMERCE_SITES:
            if host == site or host.endswith("." + site):
                return site
    except Exception:
        pass
    return "duckduckgo"


# Wine query structure (per user guidance):
#   producer (always first)  →  first-name (optional)  →  area/DOC  →  vintage (weakest)
# Stopwords must NOT include region/area names — those are strong provenance signals.
# Grape varieties and generic descriptors stay as stopwords because they're too common
# to differentiate one producer from another.
_STOPWORDS = {
    # Generic descriptors
    "wine", "wines", "vino", "bottle", "bottles", "bottiglia", "bottiglie",
    "red", "white", "rose", "rosé", "rosso", "bianco", "rosato",
    "the", "a", "an", "di", "de", "del", "della", "dei", "delle", "da",
    "il", "la", "le", "les", "los", "las", "and", "e", "et",
    # Classification labels (not the area name itself)
    "doc", "docg", "igt", "igp", "aoc", "aop", "dop",
    "riserva", "reserva", "grand", "gran", "cru", "classico", "classic",
    # Volume / vintage helpers (vintage years are stripped separately as digits)
    "vintage", "annata", "vol", "cl", "ml", "lt", "litre", "liter",
    "750ml", "magnum", "nv",
    # Producer-side generic words
    "azienda", "agricola", "cantina", "cantine", "tenuta", "tenute",
    "podere", "fattoria", "vigneti", "vigneto", "domaine", "chateau",
    # Grape varieties — too common to differentiate producers
    "shiraz", "cabernet", "sauvignon", "merlot", "pinot", "chardonnay",
    "syrah", "grenache", "tempranillo", "sangiovese", "nebbiolo", "grillo",
    "nerello", "mascalese", "aglianico", "montepulciano", "barbera", "dolcetto",
    "vermentino", "fiano", "falanghina", "viognier", "malbec", "zinfandel",
    "catarratto", "spergola", "torbato", "moscato", "passito", "bellone",
    "cannonau", "blend", "blanc", "noir", "nero", "cuvee",
    # Search/format noise
    "front", "label", "high", "resolution", "jpg", "png", "jpeg", "webp",
    "product", "image", "photo",
}


@dataclass
class WineRelevance:
    """Structured tokens parsed from a wine query for relevance matching.

    Wine query structure (per user guidance):
        producer  ["wine-name"]  [first-name]  area/DOC  [vintage]

    All fields are lowercase ASCII tokens (accents/punctuation stripped)."""
    producer: list[str] = field(default_factory=list)   # mandatory match
    wine_name: list[str] = field(default_factory=list)  # mandatory when present (quoted)
    secondary: list[str] = field(default_factory=list)  # area / first-name (bonus)


def _extract_relevance_tokens(query: str) -> WineRelevance:
    """Parse a wine query into structured relevance tokens.

    The producer is always the leading distinctive token(s) and is MANDATORY.
    A "double-quoted" phrase (or curly-quoted) is treated as the specific wine name
    and is also MANDATORY when present — this is how we distinguish two cuvées
    from the same producer (e.g. Sassicaia vs Guidalberto from Tenuta San Guido).
    Vintage years are dropped — labels rarely change year-to-year."""
    import unicodedata

    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s.lower())
        return "".join(c for c in s if not unicodedata.combining(c))

    _VOLUME_RE = re.compile(r"^\d+(ml|cl|l|lt)$")

    def _tokens(s: str) -> list[str]:
        out = []
        for t in re.findall(r"[a-z0-9]+", _norm(s)):
            if len(t) < 3:
                continue
            if t in _STOPWORDS:
                continue
            if t.isdigit():
                continue
            if _VOLUME_RE.match(t):  # 500ml, 75cl, 1l, etc.
                continue
            out.append(t)
        return out

    # Normalise curly quotes to straight ASCII so the regex catches them
    cleaned = (query.replace("\u201c", '"').replace("\u201d", '"')
                    .replace("\u2018", '"').replace("\u2019", '"'))

    quoted_phrases = re.findall(r'"([^"]+)"', cleaned)
    wine_name_tokens: list[str] = []
    for phrase in quoted_phrases:
        wine_name_tokens.extend(_tokens(phrase))

    # Strip the quoted parts so they don't double-count in producer extraction
    unquoted = re.sub(r'"[^"]*"', " ", cleaned)
    distinctive = _tokens(unquoted)

    if not distinctive and not wine_name_tokens:
        return WineRelevance()

    # Producer = first 1–2 leading tokens (excluding any quoted wine name).
    # Two-word producers are common (Anna Maria, Sella Mosca, Podere Pradarolo).
    if len(distinctive) >= 3:
        producer = distinctive[:2]
        secondary = distinctive[2:]
    elif len(distinctive) == 2:
        producer = distinctive[:1]
        secondary = distinctive[1:]
    else:
        producer = distinctive[:1]
        secondary = []

    return WineRelevance(producer=producer, wine_name=wine_name_tokens, secondary=secondary)


def _haystack_for(candidate: "Candidate") -> str:
    import unicodedata
    from urllib.parse import urlparse, unquote
    try:
        parsed = urlparse(candidate.url)
        path = unquote(parsed.path + "?" + (parsed.query or ""))
    except Exception:
        path = candidate.url
    haystack = (path + " " + (candidate.title or "")).lower()
    haystack = unicodedata.normalize("NFKD", haystack)
    haystack = "".join(c for c in haystack if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", haystack)


def _candidate_matches_relevance(candidate: "Candidate", rel: "WineRelevance") -> bool:
    """Strict wine relevance:
      1. ALL producer tokens must appear in URL/title.
      2. If a quoted wine-name was supplied, ALL wine-name tokens must also appear.
      3. If neither wine-name nor secondary tokens exist, producer alone is enough
         only when producer is highly specific (≥8 chars combined).
      4. Otherwise at least one secondary token (area / first-name) must also match.
    """
    if not rel.producer and not rel.wine_name:
        return True

    haystack = _haystack_for(candidate)

    for t in rel.producer:
        if t not in haystack:
            return False

    if rel.wine_name:
        for t in rel.wine_name:
            if t not in haystack:
                return False
        return True

    if rel.secondary:
        for t in rel.secondary:
            if t in haystack:
                return True
        # No secondary match — accept only if producer is very specific
        return sum(len(t) for t in rel.producer) >= 8

    # No secondary tokens at all (very short query) — producer alone must be specific
    return sum(len(t) for t in rel.producer) >= 8


def _probe_image_size(url: str) -> tuple[int, int]:
    """Download the first few KB of an image to read its dimensions without fetching the whole file."""
    try:
        r = requests.get(url, timeout=8, headers={"User-Agent": _BROWSER_UA, "Range": "bytes=0-65535"}, stream=True)
        if r.status_code in (200, 206):
            chunk = r.content if r.status_code == 206 else next(r.iter_content(65536), b"")
            r.close()
            if len(chunk) > 100:
                img = Image.open(io.BytesIO(chunk))
                return img.size
    except Exception:
        pass
    return (0, 0)


def score_candidate(c: Candidate, target_w: int, target_h: int, img_type: str | None) -> float:
    """Score a candidate. Higher is better. For wine, only source quality and
    image HEIGHT matter — width is irrelevant, bottle proportions are always similar."""
    s = 0.0

    # Source quality (0-100 points)
    s += SOURCE_QUALITY.get(c.source, 30)

    is_wine = img_type in WINE_TYPES
    if is_wine:
        # Wine: score purely on height — bigger is always better up to 2x target
        if c.height > 0:
            if c.height >= target_h * 1.2:
                s += 120
            elif c.height >= target_h:
                s += 100
            elif c.height >= target_h * 0.85:
                s += 70
            elif c.height >= target_h * 0.75:
                s += 40
            else:
                s += 0
        else:
            s += 20  # unknown — neutral-low
        return s

    # Non-wine: score on max dimension
    if c.width > 0 and c.height > 0:
        max_dim = max(c.width, c.height)
        target_max = max(target_w, target_h)
        if max_dim >= target_max:
            s += 100
        elif max_dim >= target_max * 0.7:
            s += 70
        elif max_dim >= target_max * 0.5:
            s += 40
        else:
            s += 10
    else:
        s += 30

    return s


# ---------------------------------------------------------------------------
# Source functions — return Candidate lists (no downloading of image bytes)
# ---------------------------------------------------------------------------

def search_vivino(query: str, limit: int = 10) -> list[Candidate]:
    """Vivino-only DuckDuckGo image search.
    Vivino has the most reliable producer-name pairing for wines."""
    search_q = f"{query} wine site:vivino.com"
    results = _ddgs_image_search(search_q, max_results=limit)
    candidates = []
    for r in results:
        url = r.get("image", "")
        if not url:
            continue
        w = int(r.get("width", 0) or 0)
        h = int(r.get("height", 0) or 0)
        title = str(r.get("title", "") or "")
        candidates.append(Candidate(url=url, source=_source_from_url(url),
                                    width=w, height=h, title=title))
    return candidates


def search_wine_ecommerce(query: str, limit: int = 8) -> list[Candidate]:
    """Search DuckDuckGo for wine bottle images across Italian and international e-commerce sites."""
    # Skip Vivino here — it's queried separately by search_vivino so it has its own slot
    sites = [s for s in WINE_ECOMMERCE_SITES if s != "vivino.com"][:8]
    site_clause = " OR ".join(f"site:{s}" for s in sites)
    search_q = f"{query} wine bottle ({site_clause})"
    results = _ddgs_image_search(search_q, max_results=limit)

    candidates = []
    for r in results:
        url = r.get("image", "")
        if not url:
            continue
        w = int(r.get("width", 0) or 0)
        h = int(r.get("height", 0) or 0)
        title = str(r.get("title", "") or "")
        candidates.append(Candidate(url=url, source=_source_from_url(url), width=w, height=h, title=title))
    return candidates


def search_duckduckgo(query: str, limit: int = 8) -> list[Candidate]:
    """General DuckDuckGo image search returning candidates."""
    results = _ddgs_image_search(query, max_results=limit)
    candidates = []
    for r in results:
        url = r.get("image", "")
        if not url:
            continue
        w = int(r.get("width", 0) or 0)
        h = int(r.get("height", 0) or 0)
        title = str(r.get("title", "") or "")
        candidates.append(Candidate(url=url, source=_source_from_url(url), width=w, height=h, title=title))
    return candidates


def search_google_cse(query: str, api_key: str, cse_id: str,
                      img_type: str | None, limit: int = 8) -> list[Candidate]:
    """Google Custom Search returning candidates."""
    params = {
        "q": query, "cx": cse_id, "key": api_key,
        "searchType": "image", "num": min(limit, 10),
    }
    if img_type == "logo":
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

    candidates = []
    for item in items[:limit]:
        url = item.get("link", "")
        img_info = item.get("image", {})
        w = int(img_info.get("width", 0) or 0)
        h = int(img_info.get("height", 0) or 0)
        if url:
            candidates.append(Candidate(url=url, source="google_cse", width=w, height=h))
    return candidates


def search_brave(query: str, api_key: str, limit: int = 8) -> list[Candidate]:
    """Brave image search returning candidates."""
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/images/search",
            params={"q": query, "count": min(limit, 10)},
            headers={"Accept": "application/json", "Accept-Encoding": "gzip", "X-Subscription-Token": api_key},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("results", [])
    except (requests.RequestException, ValueError):
        return []

    candidates = []
    for item in items[:limit]:
        url = item.get("properties", {}).get("url") or item.get("thumbnail", {}).get("src", "")
        w = int(item.get("properties", {}).get("width", 0) or 0)
        h = int(item.get("properties", {}).get("height", 0) or 0)
        if url:
            candidates.append(Candidate(url=url, source="brave", width=w, height=h))
    return candidates


# ---------------------------------------------------------------------------
# Legacy source functions (non-wine/non-product types use the old direct flow)
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> bytes | None:
    """Directly download an image from a URL (skip search)."""
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": _BROWSER_UA})
        if r.status_code == 200 and len(r.content) > 100:
            return r.content
    except requests.RequestException:
        pass
    return None


def fetch_google_favicon(domain: str) -> bytes | None:
    """Fetch high-res favicon via Google's S2 service."""
    url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 100:
            return r.content
    except requests.RequestException:
        pass
    return None


def fetch_duckduckgo_legacy(query: str, count: int) -> list[bytes]:
    """Direct DuckDuckGo fetch — used for non-wine types that don't need ranking."""
    results_raw = _ddgs_image_search(query, max_results=count + 3)
    results = []
    for img in results_raw:
        if len(results) >= count:
            break
        url = img.get("image", "")
        if not url:
            continue
        data = fetch_url(url)
        if data and len(data) > 200:
            results.append(data)
    return results


# ---------------------------------------------------------------------------
# Image processing
# ---------------------------------------------------------------------------

#: Wine processing thresholds (see user guidance).
WINE_MIN_BOTTLE_HEIGHT_PX = 800    # absolute floor: bottle bbox must be at least this tall
WINE_MIN_BBOX_COVERAGE = 0.65      # bottle must cover ≥65% of source image height (rejects lifestyle/scenic shots)
WINE_MIN_BOTTLE_ASPECT = 1.6       # h/w ≥ 1.6 excludes labels and landscape crops
WINE_OUTPUT_W = 900                # final canvas width
WINE_OUTPUT_H = 1200               # final canvas height
WINE_OUTPUT_PADDING_PX = 40        # equal top/bottom padding on output canvas


def _sample_background_color(img: Image.Image) -> tuple[int, int, int] | None:
    """Sample four corners to determine a consistent background color.
    Returns (r,g,b) if corners agree (tolerance ≤25 per channel), else None."""
    rgb = img.convert("RGB")
    w, h = rgb.size
    if w < 20 or h < 20:
        return None
    pad = 3
    corners = [
        rgb.getpixel((pad, pad)),
        rgb.getpixel((w - 1 - pad, pad)),
        rgb.getpixel((pad, h - 1 - pad)),
        rgb.getpixel((w - 1 - pad, h - 1 - pad)),
    ]
    rs = [c[0] for c in corners]
    gs = [c[1] for c in corners]
    bs = [c[2] for c in corners]
    if max(rs) - min(rs) > 25 or max(gs) - min(gs) > 25 or max(bs) - min(bs) > 25:
        return None
    return (sum(rs) // 4, sum(gs) // 4, sum(bs) // 4)


def _content_bbox(img: Image.Image, bg: tuple[int, int, int],
                  threshold: int = 28) -> tuple[int, int, int, int] | None:
    """Find the bounding box of non-background pixels. Pure-PIL implementation.
    Builds a mask of |pixel - bg| > threshold for any channel."""
    from PIL import ImageChops
    rgb = img.convert("RGB")
    # Fill-image same size as rgb with the background colour, then diff
    bg_img = Image.new("RGB", rgb.size, bg)
    diff = ImageChops.difference(rgb, bg_img)
    # Flatten to a single-channel max-of-RGB
    r, g, b = diff.split()
    max_diff = ImageChops.lighter(ImageChops.lighter(r, g), b)
    mask = max_diff.point(lambda p: 255 if p > threshold else 0)
    return mask.getbbox()


def _flatten_to_white(img: Image.Image) -> Image.Image:
    """Flatten any alpha channel onto a white background."""
    if img.mode in ("RGBA", "LA", "PA"):
        rgba = img.convert("RGBA")
        canvas = Image.new("RGB", rgba.size, (255, 255, 255))
        canvas.paste(rgba, mask=rgba.split()[-1])
        return canvas
    return img.convert("RGB")


def _replace_bg_with_white(img: Image.Image, bg: tuple[int, int, int],
                           threshold: int = 28) -> Image.Image:
    """Replace pixels close to `bg` with pure white. Preserves everything else."""
    from PIL import ImageChops
    rgb = img.convert("RGB")
    bg_img = Image.new("RGB", rgb.size, bg)
    diff = ImageChops.difference(rgb, bg_img)
    r, g, b = diff.split()
    max_diff = ImageChops.lighter(ImageChops.lighter(r, g), b)
    # content mask: 255 where pixel differs from bg, 0 where it matches
    mask = max_diff.point(lambda p: 255 if p > threshold else 0).convert("L")
    white = Image.new("RGB", rgb.size, (255, 255, 255))
    white.paste(rgb, mask=mask)
    return white


def _process_wine_image(raw: bytes) -> tuple[bytes | None, str]:
    """Wine-specific pipeline. Returns (jpg_bytes, reason_if_rejected).
    Reason is '' on success.

    Pipeline:
      1. Load + flatten to RGB/white
      2. Validate source height ≥ WINE_MIN_SOURCE_HEIGHT
      3. Sample corners for solid background (reject busy/lifestyle shots)
      4. Find content bounding box
      5. Validate bbox height ≥ WINE_MIN_BOTTLE_HEIGHT_PX
      6. Validate bbox covers ≥ WINE_MIN_BBOX_COVERAGE of source height
      7. Validate bbox aspect (h/w) ≥ WINE_MIN_BOTTLE_ASPECT (excludes labels)
      8. Replace any non-white bg with white
      9. Crop to bbox (with small safety margin)
     10. Resize to fit 1200×900 canvas with equal top/bottom padding
     11. Save as JPEG 92%
    """
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception as e:
        return None, f"pil_open_failed: {e}"

    src_w, src_h = img.size
    if src_h < WINE_MIN_SOURCE_HEIGHT:
        return None, f"source_height_too_small ({src_h}px < {WINE_MIN_SOURCE_HEIGHT}px)"

    # Flatten alpha onto white so bbox detection is consistent
    flat = _flatten_to_white(img)

    bg = _sample_background_color(flat)
    if bg is None:
        return None, "busy_or_inconsistent_background"

    bbox = _content_bbox(flat, bg)
    if bbox is None:
        return None, "no_content_detected"

    left, top, right, bottom = bbox
    bbox_h = bottom - top
    bbox_w = right - left
    if bbox_w <= 0 or bbox_h <= 0:
        return None, "empty_bbox"

    if bbox_h < WINE_MIN_BOTTLE_HEIGHT_PX:
        return None, f"bottle_too_short ({bbox_h}px < {WINE_MIN_BOTTLE_HEIGHT_PX}px)"

    coverage = bbox_h / src_h
    if coverage < WINE_MIN_BBOX_COVERAGE:
        return None, f"bottle_coverage_too_low ({coverage:.0%} < {int(WINE_MIN_BBOX_COVERAGE*100)}%)"

    aspect = bbox_h / bbox_w
    if aspect < WINE_MIN_BOTTLE_ASPECT:
        return None, f"not_bottle_shape (h/w={aspect:.2f} < {WINE_MIN_BOTTLE_ASPECT})"

    # Replace non-white background with pure white (always safe; ensures
    # the crop paste onto the final canvas is seamless).
    whitened = _replace_bg_with_white(flat, bg)

    # Crop to content bbox with a tiny 4px safety margin on each side
    margin = 4
    crop_box = (
        max(0, left - margin),
        max(0, top - margin),
        min(src_w, right + margin),
        min(src_h, bottom + margin),
    )
    cropped = whitened.crop(crop_box)

    # Resize so the bottle fits inside the output canvas minus vertical padding.
    # Preserve bottle aspect; width is free — we pad horizontally as needed.
    target_inner_h = WINE_OUTPUT_H - 2 * WINE_OUTPUT_PADDING_PX
    scale = target_inner_h / cropped.height
    new_w = max(1, int(round(cropped.width * scale)))
    new_h = target_inner_h
    resized = cropped.resize((new_w, new_h), Image.LANCZOS)

    # If resized width exceeds canvas, rescale so width fits and re-center
    if resized.width > WINE_OUTPUT_W:
        scale2 = WINE_OUTPUT_W / resized.width
        new_w2 = WINE_OUTPUT_W
        new_h2 = max(1, int(round(resized.height * scale2)))
        resized = resized.resize((new_w2, new_h2), Image.LANCZOS)

    canvas = Image.new("RGB", (WINE_OUTPUT_W, WINE_OUTPUT_H), (255, 255, 255))
    x = (WINE_OUTPUT_W - resized.width) // 2
    y = (WINE_OUTPUT_H - resized.height) // 2  # vertically centered
    canvas.paste(resized, (x, y))

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=92, optimize=True)
    return buf.getvalue(), ""


def process_image(
    raw: bytes,
    target_w: int,
    target_h: int,
    padding_pct: int,
    bg: str,
    fmt: str,
    transparent_only: bool,
    min_source_pct: int = 0,
    img_type: str | None = None,
) -> bytes | None:
    """Non-wine processing path — unchanged. Wine uses `_process_wine_image` directly."""
    try:
        img = Image.open(io.BytesIO(raw))
    except Exception:
        return None

    src_w, src_h = img.size

    if min_source_pct > 0:
        pct = min_source_pct / 100
        if src_w < target_w * pct and src_h < target_h * pct:
            return None

    if img.mode not in ("RGBA", "LA", "PA"):
        img = img.convert("RGBA")

    want_transparent = fmt in ("png", "webp") and bg in ("transparent", "none")
    if transparent_only:
        if img.mode != "RGBA" or img.getextrema()[3][0] == 255:
            return None

    if padding_pct > 0:
        scale = (100 - padding_pct) / 100
        inner_w = int(target_w * scale)
        inner_h = int(target_h * scale)
    else:
        inner_w, inner_h = target_w, target_h

    img.thumbnail((inner_w, inner_h), Image.LANCZOS)

    if want_transparent:
        canvas = Image.new("RGBA", (target_w, target_h), (0, 0, 0, 0))
    else:
        bg_color = (255, 255, 255) if bg in ("white", "transparent", "none") else (30, 30, 30)
        canvas = Image.new("RGBA", (target_w, target_h), (*bg_color, 255))

    x = (target_w - img.width) // 2
    y = (target_h - img.height) // 2
    canvas.paste(img, (x, y), img if img.mode == "RGBA" else None)

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


def _collect_candidates(
    query: str, search_q: str, img_type: str | None,
    google_key: str | None, google_id: str | None, brave_key: str | None,
) -> list[Candidate]:
    """Phase 1: Search all sources and collect candidates. No image bytes downloaded yet."""
    candidates: list[Candidate] = []
    is_wine = img_type in WINE_TYPES

    if is_wine:
        # Wine-specific waterfall:
        # 1) Vivino (most reliable producer/wine-name pairing)
        # 2) Italian + international wine e-commerce
        # 3) Wider DDG net as a fallback
        candidates.extend(search_vivino(query, limit=12))
        time.sleep(0.4)
        candidates.extend(search_wine_ecommerce(query, limit=10))
        time.sleep(0.4)
        candidates.extend(search_duckduckgo(search_q, limit=8))
        time.sleep(0.4)
    else:
        # General search
        candidates.extend(search_duckduckgo(search_q, limit=8))
        time.sleep(0.5)

    if google_key and google_id:
        candidates.extend(search_google_cse(search_q, google_key, google_id, img_type, limit=6))
        time.sleep(0.5)

    if brave_key:
        candidates.extend(search_brave(search_q, brave_key, limit=6))
        time.sleep(0.5)

    return candidates


#: Minimum source image HEIGHT (in pixels) to qualify as wine bottle product shot.
#: We don't care about width. Bottles cover most of the frame in e-commerce shots,
#: so 900px image height roughly means the bottle itself is ≥800px.
WINE_MIN_SOURCE_HEIGHT = 900


def _rank_and_probe(
    candidates: list[Candidate], target_w: int, target_h: int,
    img_type: str | None, min_source_pct: int, query: str = "",
) -> list[Candidate]:
    """Phase 2: Probe unknown sizes, apply hard filters, score, and rank.
    For wine: filter by (a) producer-strict relevance, (b) height ≥ WINE_MIN_SOURCE_HEIGHT."""
    is_wine = img_type in WINE_TYPES
    min_pct = min_source_pct / 100 if min_source_pct > 0 else 0

    # Dedup by URL first
    seen_urls: set[str] = set()
    unique = []
    for c in candidates:
        if c.url not in seen_urls:
            seen_urls.add(c.url)
            unique.append(c)
    candidates = unique

    # Strict producer + wine-name relevance for wine
    if is_wine and query:
        rel = _extract_relevance_tokens(query)
        if rel.producer or rel.wine_name:
            candidates = [c for c in candidates if _candidate_matches_relevance(c, rel)]

    # Probe dimensions for unknown-size candidates
    probe_cap = 40 if is_wine else 10
    probed = 0
    for c in candidates:
        if c.width == 0 and c.height == 0 and probed < probe_cap:
            c.width, c.height = _probe_image_size(c.url)
            probed += 1

    if is_wine:
        # Height-only hard filter. Unknown dimensions = reject.
        filtered = []
        for c in candidates:
            if c.height == 0:
                continue
            if c.height < WINE_MIN_SOURCE_HEIGHT:
                continue
            filtered.append(c)
        candidates = filtered
    elif min_pct > 0:
        min_dim = max(target_w, target_h) * min_pct
        candidates = [c for c in candidates if
                      c.width == 0 or c.height == 0 or
                      max(c.width, c.height) >= min_dim]

    for c in candidates:
        c.score = score_candidate(c, target_w, target_h, img_type)
    candidates.sort(key=lambda c: c.score, reverse=True)

    return candidates


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

    # Wine pipeline: force JPG+white output and 1200×900 canvas regardless of request.
    if img_type in WINE_TYPES:
        fmt = "jpg"
        background = "white"
        target_w, target_h = WINE_OUTPUT_W, WINE_OUTPUT_H

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

    saved = 0
    is_wine = img_type in WINE_TYPES

    def _process(raw_bytes: bytes) -> tuple[bytes | None, str]:
        if is_wine:
            return _process_wine_image(raw_bytes)
        out = process_image(
            raw_bytes, target_w, target_h, opts.padding, background or "white",
            fmt, opts.transparent_only, opts.min_source_pct, img_type,
        )
        return (out, "" if out is not None else "process_image_rejected")

    # Direct URL mode — kept for non-wine; not the focus of optimization.
    if item.url:
        data = fetch_url(item.url)
        if not data:
            failed_log.write(f"{query}\tFailed to download URL: {item.url}\n")
            stats["failed"] = 1
            return stats
        processed, reason = _process(data)
        if processed is None:
            failed_log.write(f"{query}\tDirect URL rejected: {reason}\n")
            stats["failed"] = 1
            return stats
        fname = f"{base_name}_1.{fmt}"
        (output_dir / fname).write_bytes(processed)
        stats["files"].append(fname)
        stats["downloaded"] = 1
        return stats

    # Search-rank-validate-download flow
    candidates = _collect_candidates(
        query, search_q, img_type, google_key, google_id, brave_key,
    )

    if not candidates:
        failed_log.write(f"{query}\tNo candidates found from any source\n")
        stats["failed"] = opts.count
        return stats

    ranked = _rank_and_probe(
        candidates, target_w, target_h, img_type, opts.min_source_pct, query,
    )

    if not ranked:
        if is_wine:
            failed_log.write(
                f"{query}\tNo candidates passed wine filters "
                f"(need producer match + source height ≥ {WINE_MIN_SOURCE_HEIGHT}px) "
                f"[searched={len(candidates)}]\n"
            )
        else:
            failed_log.write(f"{query}\tNo candidates passed quality + relevance filters\n")
        stats["failed"] = opts.count
        return stats

    # For wine we try up to 8x the requested count because content-bbox validation
    # rejects more aggressively than the cheap probe filter.
    attempt_limit = opts.count * (8 if is_wine else 3)
    attempts = 0
    rejection_reasons: list[str] = []

    for c in ranked:
        if saved >= opts.count or attempts >= attempt_limit:
            break
        attempts += 1

        fname = f"{base_name}_{saved + 1}.{fmt}"
        fpath = output_dir / fname

        if opts.skip_existing and fpath.exists():
            stats["skipped"] += 1
            saved += 1
            continue

        data = fetch_url(c.url)
        if not data or len(data) < 200:
            rejection_reasons.append(f"{c.source}: fetch_failed")
            continue

        processed, reason = _process(data)
        if processed is None:
            rejection_reasons.append(f"{c.source}: {reason}")
            continue

        fpath.write_bytes(processed)
        stats["files"].append(fname)
        saved += 1

    stats["downloaded"] = saved
    if saved < opts.count:
        missing = opts.count - saved - stats["skipped"]
        if missing > 0:
            stats["failed"] += missing
            tail = "; ".join(rejection_reasons[:6]) if rejection_reasons else "no_attempts"
            failed_log.write(
                f"{query}\tOnly {saved}/{opts.count} passed validation "
                f"(searched={len(candidates)}, ranked={len(ranked)}, attempted={attempts}) "
                f"reasons: {tail}\n"
            )

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
        min_src = 70 if img_type in WINE_TYPES | PRODUCT_TYPES else 0

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
        sources_available.append("Wine E-commerce (Tannico + .it)")
    if opts.type == "logo":
        sources_available.append("Google Favicon")
    if google_key and google_id:
        sources_available.append("Google CSE")
    sources_available.append("DuckDuckGo")
    if brave_key:
        sources_available.append("Brave")
    mode = "search → rank → download best" if opts.type in WINE_TYPES | PRODUCT_TYPES else "waterfall"
    print(f"Sources: {' + '.join(sources_available)} ({mode})")
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

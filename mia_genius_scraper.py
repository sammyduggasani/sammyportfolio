#!/usr/bin/env python3
"""
M.I.A. Genius Scraper → references.json  (pycountry edition)
- Reads GENIUS_TOKEN (Client Access Token) from env.
- Uses pycountry to load ALL countries (ISO-3166).
- Adds informal/historical aliases (e.g., Burma → Myanmar).
- Supports regex patterns for city nicknames safely (ATL, L.A., etc.).
- Scrapes lyrics from Genius song pages and matches mentions.
"""
import os, re, time, json, argparse
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
import pycountry

GENIUS_API = "https://api.genius.com"

# ----------------------- Country scaffolding -----------------------

def seed_countries_from_pycountry() -> Dict[str, Dict[str, Any]]:
    """Base: every ISO-3166 country with empty alias/language/city/pattern lists."""
    base = {}
    for c in pycountry.countries:
        # Some entries lack alpha_3 (rare), skip those
        if not getattr(c, "alpha_3", None): 
            continue
        base[c.alpha_3] = {
            "name": getattr(c, "common_name", None) or c.name,
            "aliases": [],         # plain string aliases (word-boundary matched)
            "languages": [],       # plain language tokens
            "cities": [],          # plain city tokens
            "patterns": [],        # explicit regex strings (use for abbreviations)
        }
    return base

# Informal / historical / common alternates
EXTRA_ALIASES = {
    "MMR": ["Burma"],  # Myanmar
    "NLD": ["Holland", "The Netherlands"],
    "CIV": ["Ivory Coast", "Côte d’Ivoire", "Cote d’Ivoire"],
    "SWZ": ["Swaziland"],            # Eswatini has SWZ in ISO
    "TUR": ["Turkey", "Türkiye"],    # ISO name is Türkiye/Turkey
    "COD": ["Congo-Kinshasa", "DRC", "Democratic Republic of Congo"],
    "COG": ["Congo-Brazzaville", "Republic of Congo"],
    "RUS": ["Russia"],               # ISO name often “Russian Federation”
    "GBR": ["UK", "Britain", "Great Britain", "England", "Scotland", "Wales"],
    "IRN": ["Persia"],
    "LAO": ["Laos"],
    "PRK": ["North Korea", "DPRK"],
    "KOR": ["South Korea", "ROK"],
    "TZA": ["Tanzania"],             # ISO sometimes uses “United Republic of Tanzania”
    "USA": ["U.S.", "US", "U.S.A.", "America"],
    "PSE": ["Palestine", "State of Palestine"],
    "VAT": ["Vatican", "Holy See"],
    "CZE": ["Czech Republic", "Czechia"],
    "SWZ": ["Eswatini", "Swaziland"],
    "MKD": ["Macedonia", "North Macedonia"],
    "BRN": ["Brunei Darussalam", "Brunei"],
    "BOL": ["Bolivia"],
    "MDA": ["Moldova", "Republic of Moldova"],
}

# You can add languages/cities you care about here
EXTRA_ENRICHMENTS = {
    "IND": {
        "languages": ["tamil","hindi","urdu","punjabi","bengali","telugu","marathi"],
        "cities": ["mumbai","bombay","delhi","new delhi","chennai","madras","hyderabad","kolkata","calcutta","bangalore","bengaluru"],
        "aliases": ["Bharat","Hindustan"],
    },
    "LKA": {
        "languages": ["sinhala","tamil"],
        "cities": ["colombo","kandy","jaffna"],
        "aliases": ["Ceylon"],
    },
    "JAM": {
        "languages": ["patois","jamaican patois"],
        "cities": ["kingston","montego bay"],
    },
    "GBR": {
        "languages": ["english"],
        "cities": ["london","brixton","camden","hackney","ealing"],
    },
    "USA": {
        "languages": ["english","spanish"],
        # avoid ambiguous two-letter tokens like "la"
        "cities": [
            "new york","nyc",
            "los angeles",      # use explicit form
            "washington","washington dc","dc","d.c.",
            "san francisco","sf","bay area",
            "atlanta"           # see regex patterns for ATL, L.A., etc.
        ],
        # Safe regex patterns for abbreviations/nicknames
        "patterns": [
            r"\bATL\b",                     # Atlanta
            r"\bL\.A\.\b",                  # L.A.
            r"\bD\.C\.\b",                  # D.C.
            r"\bNOLA\b",                    # New Orleans
            r"\bDMV\b",                     # DC–MD–VA region
            r"\bCHI\b",                     # Chicago (slang)
            r"\bVEGAS\b",                   # Las Vegas
        ],
    },
    "BGD": {
        "languages": ["bengali","bangla"],
        "cities": ["dhaka","chittagong"],
    },
    "PAK": {
        "languages": ["urdu","punjabi","sindhi"],
        "cities": ["karachi","lahore","islamabad"],
    },
    "TTO": {
        "languages": ["english","creole"],
        "cities": ["port of spain"],
    },
    "FRA": {
        "languages": ["french"],
        "cities": ["paris","marseille"],
    },
    "CAN": {
        "languages": ["english","french"],
        "cities": ["toronto","montreal"],
    },
}

# Global single-word language hints → iso3 (heuristics)
GLOBAL_LANGUAGE_HINTS = {
    "tamil": "IND",
    "patois": "JAM",
    "jamaican patois": "JAM",
    "sinhala": "LKA",
    "bangla": "BGD",
    "bengali": "BGD",
    "urdu": "PAK",
    "punjabi": "IND",  # heuristic; could map to PAK as well
}

def build_country_index() -> Dict[str, Dict[str, Any]]:
    countries = seed_countries_from_pycountry()

    # merge aliases
    for iso3, aliases in EXTRA_ALIASES.items():
        if iso3 in countries:
            countries[iso3]["aliases"].extend(aliases)

    # merge enrichments (languages, cities, patterns, extra aliases)
    for iso3, enrich in EXTRA_ENRICHMENTS.items():
        if iso3 not in countries:
            continue
        for k in ("languages", "cities", "patterns", "aliases"):
            if k in enrich:
                countries[iso3][k].extend(enrich[k])

    # de-dup & lowercase simple lists
    for iso3, info in countries.items():
        for key in ("aliases", "languages", "cities"):
            info[key] = sorted(set(s.lower() for s in info[key]))
        # patterns are regex strings; keep as-is (but unique)
        info["patterns"] = sorted(set(info["patterns"]))

    return countries

COUNTRIES = build_country_index()

# ----------------------- Genius bits -----------------------

def get_token() -> str:
    token = os.getenv("GENIUS_TOKEN")
    if not token:
        raise SystemExit("Missing GENIUS_TOKEN environment variable.")
    return token

def genius_get(path: str, params: dict) -> dict:
    headers = {"Authorization": f"Bearer {get_token()}"}
    r = requests.get(f"{GENIUS_API}{path}", headers=headers, params=params, timeout=20)
    if r.status_code == 429:
        time.sleep(3)
        r = requests.get(f"{GENIUS_API}{path}", headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def search_mia_songs(query="M.I.A.", limit=150) -> List[dict]:
    results, page, fetched = {}, 1, 0
    while fetched < limit:
        data = genius_get("/search", {"q": query, "page": page})
        section = data.get("response", {}).get("hits", [])
        if not section: break
        for hit in section:
            res = hit.get("result", {})
            pa = res.get("primary_artist", {})
            if (pa.get("name") or "").lower().strip() in {"m.i.a.", "mia"}:
                sid = res.get("id")
                if sid not in results:
                    results[sid] = {
                        "id": sid,
                        "title": res.get("title"),
                        "url": res.get("url"),
                        "release_date_for_display": res.get("release_date_for_display"),
                        "primary_artist": pa.get("name"),
                    }
                    fetched += 1
                    if fetched >= limit: break
        page += 1
        time.sleep(0.5)
    return list(results.values())

def fetch_lyrics_from_url(url: str) -> str:
    try:
        html = requests.get(url, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.select('div[data-lyrics-container="true"]')
        if blocks:
            return "\n".join(b.get_text(separator="\n") for b in blocks)
        lyr = soup.find("div", class_="lyrics")
        return lyr.get_text(separator="\n") if lyr else ""
    except Exception:
        return ""

# ----------------------- Match logic -----------------------

def extract_snippet(text: str, terms: List[str], window: int = 80) -> str:
    low = text.lower()
    for t in terms:
        idx = low.find(t.lower())
        if idx != -1:
            start = max(0, idx - window)
            end = min(len(text), idx + len(t) + window)
            snippet = text[start:end].strip().replace("\n", " ")
            return re.sub(r"\s+", " ", snippet)
    return ""

def safe_word_regex(token: str) -> re.Pattern:
    """Build a case-insensitive word-boundary regex for a plain token."""
    return re.compile(rf"\b{re.escape(token)}\b", flags=re.IGNORECASE)

def find_matches(lyrics: str, meta_text: str, countries=COUNTRIES) -> Dict[str, List[Dict[str, Any]]]:
    found: Dict[str, List[Dict[str, Any]]] = {}
    fulltext = f"{lyrics}\n{meta_text}"

    for iso3, info in countries.items():
        types_for_country = []

        # 1) country name & plain aliases/cities
        plain_terms = [info["name"]] + info.get("aliases", []) + info.get("cities", [])
        if any(safe_word_regex(t).search(fulltext) for t in plain_terms if t):
            snippet = extract_snippet(lyrics, plain_terms)
            types_for_country.append({"type": "mention", "lyric": snippet})

        # 2) explicit regex patterns (for ATL, L.A., etc.)
        for pat in info.get("patterns", []):
            if re.search(pat, fulltext, flags=re.IGNORECASE):
                snippet = extract_snippet(lyrics, [pat])
                types_for_country.append({"type": "mention", "lyric": snippet})

        # 3) language references (plain tokens)
        for lang in info.get("languages", []):
            if safe_word_regex(lang).search(fulltext):
                snippet = extract_snippet(lyrics, [lang])
                types_for_country.append({"type": "language", "lyric": snippet, "language": lang})

        # 4) global language hints (heuristic)
        for key, hint_iso in GLOBAL_LANGUAGE_HINTS.items():
            if hint_iso == iso3 and safe_word_regex(key).search(fulltext):
                snippet = extract_snippet(lyrics, [key])
                types_for_country.append({"type": "language", "lyric": snippet, "language": key})

        if types_for_country:
            found[iso3] = types_for_country

    return found

# ----------------------- Main -----------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=120)
    ap.add_argument("--add-samples", type=str, default=None, help="CSV with sample origins: iso3,song,source,year,album,audio,country,language")
    ap.add_argument("--out", type=str, default="data/references.json")
    args = ap.parse_args()

    _ = get_token()  # ensure present

    print("Searching Genius for M.I.A. songs...")
    songs = search_mia_songs(limit=args.limit)
    print(f"Found ~{len(songs)} candidate songs. Fetching lyrics...")

    rows = []
    for i, s in enumerate(songs, 1):
        time.sleep(0.7)
        lyrics = fetch_lyrics_from_url(s["url"])
        if not lyrics:
            continue
        matches = find_matches(lyrics, f"{s['title']} {s.get('release_date_for_display','') or ''}")
        if not matches:
            continue
        for iso3, types in matches.items():
            for t in types:
                rows.append({
                    "iso3": iso3,
                    "country": COUNTRIES[iso3]["name"],
                    "type": t["type"],
                    "song": s["title"],
                    "album": None,
                    "year": s.get("release_date_for_display"),
                    "lyric": t.get("lyric", ""),
                    "language": t.get("language"),
                    "source": None,
                    "audio": None,
                })

    # Merge optional sample-origin CSV
    if args.add_samples:
        import csv
        with open(args.add_samples, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append({
                    "iso3": r["iso3"],
                    "country": r.get("country") or COUNTRIES.get(r["iso3"], {}).get("name", r["iso3"]),
                    "type": "sample",
                    "song": r.get("song"),
                    "album": r.get("album"),
                    "year": r.get("year"),
                    "lyric": "",
                    "language": r.get("language"),
                    "source": r.get("source"),
                    "audio": r.get("audio"),
                })

    # Collapse to map shape
    by_country: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = r["iso3"]
        by_country.setdefault(key, {"iso3": r["iso3"], "country": r["country"], "refs": []})
        by_country[key]["refs"].append({
            "type": r["type"],
            "song": r["song"],
            "album": r.get("album"),
            "year": r.get("year"),
            "lyric": r.get("lyric", ""),
            "language": r.get("language"),
            "source": r.get("source"),
            "audio": r.get("audio"),
        })

    out = sorted(by_country.values(), key=lambda x: x["country"])
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(out)} countries → {args.out}")
    print("Next: add short audio clips and wire to your D3 map.")

if __name__ == "__main__":
    main()


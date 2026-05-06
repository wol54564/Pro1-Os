"""
Test: 403 bypass using scrape.do for the Animals page.

Usage:
    SCRAPEDO_TOKEN=<token> python test_403_fix.py
    # or pass directly:
    python test_403_fix.py --token fbfd397bd01a4e4789a24e0fc7f428345291ebcaf96

The test:
  1. Requests the Animals listing page through scrape.do.
  2. Confirms HTTP 200 is returned (not 403).
  3. Parses the __NEXT_DATA__ JSON embedded in the HTML.
  4. Prints the number of subcategories found so we know real data arrived.
"""

import argparse
import json
import os
import sys
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

ANIMALS_URL = "https://www.q84sale.com/ar/animals"
SCRAPEDO_ENDPOINT = "https://api.scrape.do"


def fetch_via_scrapedo(target_url: str, token: str) -> requests.Response:
    """Fetch *target_url* through scrape.do trying multiple strategies.

    q84sale is a Next.js SSR site — __NEXT_DATA__ is in the raw HTML, so JS
    rendering is not required.  We try strategies in order of cost/complexity:

      1. super=true only   — residential proxy, plain HTTP (cheapest)
      2. render=true only  — headless Chrome, datacenter proxy
      3. render+super      — headless Chrome + residential proxy (most expensive)
    """
    strategies = [
        {"super": "true", "geoCode": "kw"},
        {"render": "true", "geoCode": "kw"},
        {"render": "true", "super": "true", "geoCode": "kw"},
    ]

    for i, params in enumerate(strategies, 1):
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        api_url = f"{SCRAPEDO_ENDPOINT}?token={token}&url={quote_plus(target_url)}&{qs}"
        label = ", ".join(f"{k}={v}" for k, v in params.items())
        print(f"  Strategy {i} ({label}) ...")
        try:
            resp = requests.get(api_url, timeout=120)
        except Exception as exc:
            print(f"    Request error: {exc}")
            continue

        print(f"  → HTTP {resp.status_code}")

        if resp.status_code == 200:
            return resp

        # scrape.do 502/4xx with a JSON error body — log and try next strategy
        try:
            err = resp.json()
            msg = err.get("Message", resp.text[:200])
        except Exception:
            msg = resp.text[:200]
        print(f"    scrape.do error: {msg}")

    # Return the last response so the caller can surface the final status code
    return resp


def parse_next_data(html: str) -> dict:
    """Extract and parse the __NEXT_DATA__ JSON block from the page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script or not script.string:
        return {}
    return json.loads(script.string)


def main():
    parser = argparse.ArgumentParser(description="Test 403 fix via scrape.do for Animals page")
    parser.add_argument(
        "--token",
        default=os.environ.get("SCRAPEDO_TOKEN", ""),
        help="scrape.do API token (or set SCRAPEDO_TOKEN env var)",
    )
    parser.add_argument(
        "--url",
        default=ANIMALS_URL,
        help="Target URL to test (default: Animals page)",
    )
    args = parser.parse_args()

    token = args.token
    target = args.url

    if not token:
        print("ERROR: No scrape.do token provided. Use --token or set SCRAPEDO_TOKEN env var.")
        sys.exit(1)

    print(f"Target URL : {target}")
    print(f"Token      : {token[:8]}...{token[-4:]}  (masked)")
    print("-" * 50)

    # ── Step 1: fetch via scrape.do ──────────────────────────────────────────
    print("Sending request through scrape.do ...")
    try:
        resp = fetch_via_scrapedo(target, token)
    except Exception as exc:
        print(f"FAIL  Request error: {exc}")
        sys.exit(1)

    print(f"HTTP status : {resp.status_code}")

    if resp.status_code == 403:
        print("FAIL  Still getting 403 — token may be invalid or quota exhausted.")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"FAIL  All strategies failed. Last status code: {resp.status_code}")
        sys.exit(1)

    print("PASS  HTTP 200 received — 403 is bypassed!")

    # ── Step 2: parse __NEXT_DATA__ ──────────────────────────────────────────
    print("\nParsing __NEXT_DATA__ JSON ...")
    data = parse_next_data(resp.text)

    if not data:
        print("WARN  __NEXT_DATA__ block not found — page may have changed structure.")
        sys.exit(1)

    subcats = (
        data.get("props", {})
        .get("pageProps", {})
        .get("verticalSubcats", [])
    )

    if subcats:
        print(f"PASS  Found {len(subcats)} subcategories in __NEXT_DATA__:")
        for sc in subcats:
            name = sc.get("name_en") or sc.get("name_ar") or sc.get("slug")
            count = sc.get("listings_count", "?")
            print(f"       - {name}  ({count} listings)")
    else:
        # Try to show top-level keys so the user can debug the structure
        props_keys = list(data.get("props", {}).get("pageProps", {}).keys())
        print(f"WARN  No verticalSubcats found. pageProps keys: {props_keys}")

    print("\n[TEST COMPLETE] scrape.do successfully bypasses 403 for the Animals page.")


if __name__ == "__main__":
    main()

import json
from contextlib import contextmanager
from datetime import date

import pytest

from website_ads_counter import count_4sale_ads_published_on


def _html(page_props):
    payload = json.dumps({"props": {"pageProps": page_props}}).encode("utf-8")
    return b'<script id="__NEXT_DATA__" type="application/json">' + payload + b"</script>"


def test_counts_target_date_and_deduplicates_ads(monkeypatch):
    pages = {
        "root/1": {
            "verticalSubcats": [{"slug_url": "root/leaf/1"}],
        },
        "root/leaf/1": {
            "totalPages": 4,
            "listings": [
                {"id": 10, "date_published": "2026-11-08 12:00:00"},
                {"id": 20, "date_published": "2026-11-09 01:00:00"},
                {"id": 99, "date_published": "2026-11-08 11:00:00", "adBoxId": "ad"},
            ],
        },
        "root/leaf/2": {
            "totalPages": 4,
            "listings": [
                {"id": 10, "date_published": "2026-11-08 12:00:00"},
                {"id": 11, "date_published": "2026-11-08 10:00:00"},
            ],
        },
        "root/leaf/3": {
            "totalPages": 4,
            "listings": [{"id": 12, "date_published": "2026-11-07 23:59:59"}],
        },
    }
    requested = []

    @contextmanager
    def fake_urlopen(request, timeout):
        path = request.full_url.split("/ar/", 1)[1]
        requested.append(path)
        yield type("Response", (), {"read": lambda self: _html(pages[path])})()

    monkeypatch.setattr("website_ads_counter._CATEGORY_ROOTS", ("root",))
    monkeypatch.setattr("website_ads_counter.urllib.request.urlopen", fake_urlopen)

    result = count_4sale_ads_published_on("2026-11-08")

    assert result == {
        "count": 2,
        "categories_scanned": 2,
        "pages_scanned": 4,
        "source": "q84sale_public_listing_pages",
    }
    assert requested == ["root/1", "root/leaf/1", "root/leaf/2", "root/leaf/3"]


def test_raises_when_website_page_fails(monkeypatch):
    def fail_urlopen(request, timeout):
        raise OSError("website unavailable")

    monkeypatch.setattr("website_ads_counter._CATEGORY_ROOTS", ("root",))
    monkeypatch.setattr("website_ads_counter.urllib.request.urlopen", fail_urlopen)

    monkeypatch.setattr("website_ads_counter.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="website unavailable"):
        count_4sale_ads_published_on(date(2026, 11, 8).isoformat())


def test_empty_website_results_are_a_successful_zero(monkeypatch):
    @contextmanager
    def fake_urlopen(request, timeout):
        yield type(
            "Response",
            (),
            {"read": lambda self: _html({"listings": [], "verticalSubcats": []})},
        )()

    monkeypatch.setattr("website_ads_counter._CATEGORY_ROOTS", ("root",))
    monkeypatch.setattr("website_ads_counter.urllib.request.urlopen", fake_urlopen)

    result = count_4sale_ads_published_on("2026-11-08")

    assert result["count"] == 0
    assert result["categories_scanned"] == 1
    assert result["pages_scanned"] == 1

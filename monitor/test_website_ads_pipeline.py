from aggregate_reports import summarize_site
from export_hub_tables import SITE_DAILY_COLS, flatten_hub


def test_website_ads_count_flows_into_site_daily_export():
    website_metrics = {
        "website_published_ads": 23,
        "website_published_ads_date": "2026-11-08",
        "website_published_ads_source": "q84sale_public_listing_pages",
        "website_published_ads_status": "ok",
    }
    report = {
        "run_date": "2026-11-09",
        "inspect_date": "2026-11-08",
        "total_unique_ads": 31,
        "scrapers": [{"scraper": "cars", "unique_ads": 31, "all_passed": True}],
        **website_metrics,
    }
    site = {
        "folder": "4sale",
        "site_id": "q84sale-kw",
        "display_name": "4Sale Kuwait",
        "scrapers": 1,
    }

    summary = summarize_site(report, site, "2026-11-09", "2026-11-09", False)
    _, site_rows, *_ = flatten_hub(
        {"run_date": "2026-11-09", "sites": [summary]}
    )

    assert site_rows[0]["unique_ads"] == 31
    for key, value in website_metrics.items():
        assert site_rows[0][key] == value
        assert key in SITE_DAILY_COLS

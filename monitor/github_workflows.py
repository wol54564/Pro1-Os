"""
Resolve scraper pipeline metadata from site/registry config and GitHub Actions API.

The Schema Monitor workflow must not appear as the site's CI workflow in the dashboard;
sites declare their scraper workflows in registry.yml or site.yml.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("monitor")

MONITOR_WORKFLOW_NAMES = frozenset({
    "Schema Monitor",
    "Monitor Hub — Aggregate All Sites",
    "monitor",
})


def is_monitor_workflow(name: Optional[str]) -> bool:
    if not name:
        return False
    if name in MONITOR_WORKFLOW_NAMES:
        return True
    return name.startswith("Monitor Hub")


def resolve_workflow_names(site: Dict) -> List[str]:
    """Ordered scraper workflow display names for a site (from registry or site.yml)."""
    raw = site.get("workflows")
    if raw is None:
        single = site.get("workflow_name")
        if single and not is_monitor_workflow(str(single)):
            return [str(single)]
        return []

    if isinstance(raw, str):
        parts = [p.strip() for p in raw.replace("→", ",").split(",") if p.strip()]
        return [p for p in parts if not is_monitor_workflow(p)]

    if isinstance(raw, list):
        return [str(w).strip() for w in raw if w and not is_monitor_workflow(str(w))]

    return []


def format_workflow_label(names: List[str]) -> str:
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    return " → ".join(names)


def merge_registry_site(site: Dict, registry: Optional[Dict]) -> Dict:
    """Overlay registry fields onto site config for metadata resolution."""
    if not registry:
        return site

    folder = site.get("folder")
    site_id = site.get("site_id")
    reg_row: Dict = {}
    for row in registry.get("sites", []):
        if folder and row.get("folder") == folder:
            reg_row = row
            break
        if site_id and row.get("site_id") == site_id:
            reg_row = row
            break

    if not reg_row:
        return site

    merged = dict(site)
    for key in (
        "github_username",
        "repo",
        "run_place",
        "schedule",
        "workflows",
        "workflow_name",
    ):
        if merged.get(key) in (None, "", []):
            val = reg_row.get(key)
            if val not in (None, "", []):
                merged[key] = val
    return merged


def _github_request(url: str, token: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "pro1-os-schema-monitor",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_github_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _run_duration_sec(run: Dict) -> int:
    started = _parse_github_dt(run.get("run_started_at"))
    finished = _parse_github_dt(run.get("updated_at"))
    if started and finished:
        return max(0, int((finished - started).total_seconds()))
    return 0


def _workflow_name_map(owner: str, repo: str, token: str) -> Dict[str, int]:
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows?per_page=100"
    data = _github_request(url, token)
    mapping: Dict[str, int] = {}
    for wf in data.get("workflows", []):
        name = wf.get("name")
        wf_id = wf.get("id")
        if name and wf_id:
            mapping[str(name)] = int(wf_id)
    return mapping


def _latest_run_for_workflow(
    owner: str,
    repo: str,
    workflow_id: int,
    token: str,
    not_before: datetime,
) -> Optional[Dict]:
    url = (
        f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/"
        f"{workflow_id}/runs?per_page=15&exclude_pull_requests=true"
    )
    data = _github_request(url, token)
    for run in data.get("workflow_runs", []):
        if run.get("status") != "completed":
            continue
        started = _parse_github_dt(run.get("run_started_at"))
        if started and started >= not_before:
            return run
    return None


def _pipeline_status(conclusions: List[Optional[str]]) -> str:
    if not conclusions:
        return "unknown"
    for conclusion in conclusions:
        if conclusion in (None, "cancelled"):
            continue
        if conclusion not in ("success", "skipped"):
            return "failure"
    return "success"


def fetch_pipeline_github_meta(
    site: Dict,
    partition_date: str,
    token: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch latest GitHub Actions runs for the site's configured scraper workflows.

    partition_date: hub/report partition (YYYY-MM-DD), when Batch A typically starts.
    """
    workflow_names = resolve_workflow_names(site)
    if not workflow_names:
        return None

    owner = (site.get("github_username") or site.get("github_owner") or "").strip()
    repo = (site.get("repo") or "").strip()
    token = (token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if not owner or not repo:
        log.info("Skipping GitHub workflow lookup — missing github_username or repo")
        return None
    if not token:
        log.info("Skipping GitHub workflow lookup — no GITHUB_TOKEN")
        return None

    try:
        not_before = datetime.strptime(partition_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        not_before -= timedelta(hours=6)
    except ValueError:
        not_before = datetime.now(timezone.utc) - timedelta(days=1)

    try:
        name_to_id = _workflow_name_map(owner, repo, token)
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        log.warning(f"GitHub workflow list failed for {owner}/{repo}: {exc}")
        return None

    runs_detail: List[Dict[str, Any]] = []
    for wf_name in workflow_names:
        wf_id = name_to_id.get(wf_name)
        if not wf_id:
            log.warning(f"Workflow not found in {owner}/{repo}: {wf_name!r}")
            continue
        try:
            run = _latest_run_for_workflow(owner, repo, wf_id, token, not_before)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            log.warning(f"GitHub run lookup failed for {wf_name}: {exc}")
            continue
        if not run:
            log.info(f"No recent GitHub run for {wf_name} since {not_before.isoformat()}")
            continue
        runs_detail.append({
            "name": wf_name,
            "run_id": run.get("id"),
            "run_number": run.get("run_number"),
            "conclusion": run.get("conclusion"),
            "status": run.get("status"),
            "duration_sec": _run_duration_sec(run),
            "run_started_at": run.get("run_started_at"),
            "updated_at": run.get("updated_at"),
            "html_url": run.get("html_url"),
        })

    if not runs_detail:
        return None

    found_names = [r["name"] for r in runs_detail]
    label_names = found_names if len(found_names) == len(workflow_names) else workflow_names
    workflow_name = format_workflow_label(label_names)
    conclusions = [r.get("conclusion") for r in runs_detail]
    last_run = runs_detail[-1]

    return {
        "run_place": "github",
        "workflow_name": workflow_name,
        "workflow_status": _pipeline_status(conclusions),
        "duration_sec": sum(r.get("duration_sec") or 0 for r in runs_detail),
        "workflow_run_id": str(last_run["run_id"]) if last_run.get("run_id") else None,
        "workflow_run_number": last_run.get("run_number"),
        "github_repository": f"{owner}/{repo}",
        "workflows": runs_detail,
        "source": "github_api",
    }


def build_scraper_run_meta(
    site: Dict,
    partition_date: str,
    monitor_started_at: datetime,
    validation_passed: bool,
) -> Dict[str, Any]:
    """
    Build github_run metadata for report.json / dashboard.

    Prefers scraper pipeline runs from GitHub API; never labels the monitor workflow
    as the site's primary workflow.
    """
    pipeline = fetch_pipeline_github_meta(site, partition_date)

    monitor_meta: Dict[str, Any] = {
        "run_place": (site.get("run_place") or "github").strip().lower(),
        "workflow_name": os.environ.get("GITHUB_WORKFLOW", "Schema Monitor"),
        "workflow_status": "success" if validation_passed else "failure",
        "duration_sec": max(0, int((datetime.utcnow() - monitor_started_at).total_seconds())),
        "started_at": monitor_started_at.isoformat() + "Z",
        "finished_at": datetime.utcnow().isoformat() + "Z",
    }
    if os.environ.get("GITHUB_ACTIONS") == "true":
        monitor_meta.update({
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "workflow_run_number": int(os.environ.get("GITHUB_RUN_NUMBER") or 0),
            "github_repository": os.environ.get("GITHUB_REPOSITORY", ""),
        })

    if pipeline:
        result = dict(pipeline)
        result["monitor_run"] = monitor_meta
        return result

    configured = resolve_workflow_names(site)
    fallback_name = format_workflow_label(configured) if configured else None
    if not fallback_name:
        legacy = site.get("workflow_name")
        if legacy and not is_monitor_workflow(str(legacy)):
            fallback_name = str(legacy)

    run_place = (site.get("run_place") or "github").strip().lower()
    return {
        "run_place": run_place,
        "workflow_name": fallback_name or "—",
        "workflow_status": None,
        "duration_sec": None,
        "monitor_run": monitor_meta,
        "source": "registry_fallback",
    }

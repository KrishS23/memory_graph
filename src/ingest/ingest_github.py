from __future__ import annotations

import os
import json
import time
import hashlib
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def stable_artifact_id(source: str, repo: str, kind: str, primary_key: str) -> str:
    return sha256_hex(f"{source}|{repo}|{kind}|{primary_key}")


def clean_text_github(text: Optional[str]) -> str:
    if not text:
        return ""
    t = text.replace("\r\n", "\n").strip()
    while "\n\n\n" in t:
        t = t.replace("\n\n\n", "\n\n")
    return t


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def load_selected_issues(path: str) -> List[int]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class GitHubClient:
    def __init__(self, token: str, api_version: str = "2022-11-28"):
        self.token = token
        self.api_version = api_version
        self.base = "https://api.github.com"

    def headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": self.api_version,
            "User-Agent": "layer10-ingestor",
        }

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{self.base}{path}"
        return requests.get(url, headers=self.headers(), params=params, timeout=60)

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        while True:
            resp = self.get(path, params=params)

            if resp.status_code in (403, 429):
                reset = resp.headers.get("x-ratelimit-reset")
                if reset:
                    sleep_for = max(0, int(reset) - int(time.time()) + 2)
                    print(f"Rate limited. Sleeping {sleep_for}s...")
                    time.sleep(sleep_for)
                    continue
                print("Rate limited. Sleeping 60s...")
                time.sleep(60)
                continue

            resp.raise_for_status()
            return resp.json()

    def paginate(self, path: str, params: Dict[str, Any]) -> List[Any]:
        all_items: List[Any] = []
        page = 1
        while True:
            p = dict(params)
            p["page"] = page
            resp = self.get(path, params=p)

            if resp.status_code in (403, 429):
                reset = resp.headers.get("x-ratelimit-reset")
                if reset:
                    sleep_for = max(0, int(reset) - int(time.time()) + 2)
                    print(f"Rate limited. Sleeping {sleep_for}s...")
                    time.sleep(sleep_for)
                    continue
                time.sleep(60)
                continue

            resp.raise_for_status()
            items = resp.json()
            if not items:
                break
            all_items.extend(items)
            if len(items) < p.get("per_page", 100):
                break
            page += 1
        return all_items


def main() -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN in .env")

    owner = "rust-lang"
    repo = "rust"
    repo_full = f"{owner}/{repo}"

    selected_path = "data/raw/selected_issues.json"
    out_path = "data/raw/artifacts.jsonl"
    stats_path = "data/raw/ingest_stats.json"

    issue_numbers = load_selected_issues(selected_path)
    gh = GitHubClient(token=token)

    total_issue = 0
    total_comment = 0
    total_event = 0

    # start fresh each run for now
    if os.path.exists(out_path):
        os.remove(out_path)

    for idx, num in enumerate(issue_numbers, start=1):
        issue = gh.get_json(f"/repos/{owner}/{repo}/issues/{num}")

        issue_pk = f"issue#{num}"
        issue_id = stable_artifact_id("github", repo_full, "issue", issue_pk)

        issue_row = {
            "artifact_id": issue_id,
            "type": "issue",
            "source": "github",
            "repo": repo_full,
            "issue_number": num,
            "url": issue.get("html_url"),
            "author": (issue.get("user") or {}).get("login"),
            "created_at": issue.get("created_at"),
            "updated_at": issue.get("updated_at"),
            "raw_text": issue.get("body") or "",
            "clean_text": clean_text_github(issue.get("body")),
            "metadata": {
                "title": issue.get("title"),
                "state": issue.get("state"),
                "labels": [l.get("name") for l in (issue.get("labels") or []) if isinstance(l, dict)],
                "assignees": [a.get("login") for a in (issue.get("assignees") or []) if isinstance(a, dict)],
                "comments": issue.get("comments"),
                "closed_at": issue.get("closed_at"),
            },
        }
        write_jsonl(out_path, [issue_row])
        total_issue += 1

        comments = gh.paginate(
            f"/repos/{owner}/{repo}/issues/{num}/comments",
            {"per_page": 100},
        )

        comment_rows = []
        for c in comments:
            cid = c.get("id")
            pk = f"issue#{num}/comment#{cid}"
            aid = stable_artifact_id("github", repo_full, "comment", pk)
            comment_rows.append({
                "artifact_id": aid,
                "type": "comment",
                "source": "github",
                "repo": repo_full,
                "issue_number": num,
                "url": c.get("html_url"),
                "author": (c.get("user") or {}).get("login"),
                "created_at": c.get("created_at"),
                "updated_at": c.get("updated_at"),
                "raw_text": c.get("body") or "",
                "clean_text": clean_text_github(c.get("body")),
                "metadata": {},
            })
        if comment_rows:
            write_jsonl(out_path, comment_rows)
        total_comment += len(comment_rows)

        events = gh.paginate(
            f"/repos/{owner}/{repo}/issues/{num}/events",
            {"per_page": 100},
        )

        event_rows = []
        for e in events:
            eid = e.get("id")
            pk = f"issue#{num}/event#{eid}"
            aid = stable_artifact_id("github", repo_full, "event", pk)

            event_rows.append({
                "artifact_id": aid,
                "type": "event",
                "source": "github",
                "repo": repo_full,
                "issue_number": num,
                "url": issue.get("html_url"),
                "author": (e.get("actor") or {}).get("login"),
                "created_at": e.get("created_at"),
                "updated_at": e.get("created_at"),
                "raw_text": "",
                "clean_text": "",
                "metadata": {
                    "event": e.get("event"),
                    "label": (e.get("label") or {}).get("name") if isinstance(e.get("label"), dict) else None,
                    "assignee": (e.get("assignee") or {}).get("login") if isinstance(e.get("assignee"), dict) else None,
                    "assigner": (e.get("assigner") or {}).get("login") if isinstance(e.get("assigner"), dict) else None,
                    "commit_id": e.get("commit_id"),
                    "commit_url": e.get("commit_url"),
                },
            })
        if event_rows:
            write_jsonl(out_path, event_rows)
        total_event += len(event_rows)

        if idx % 25 == 0:
            print(f"Processed {idx}/{len(issue_numbers)} issues...")

    stats = {
        "repo": repo_full,
        "issues": total_issue,
        "comments": total_comment,
        "events": total_event,
        "output": out_path,
    }
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print("\n✅ Ingestion complete")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
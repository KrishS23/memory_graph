import json
import os
import hashlib
from typing import Any, Dict, List

INPUT_PATH = "data/processed/artifacts_deduped.jsonl"
CLAIMS_OUT = "data/processed/claims.jsonl"
EVIDENCE_OUT = "data/processed/evidence.jsonl"
REPORT_OUT = "data/processed/extract_report.json"


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_issue_entity_id(repo: str, issue_number: int) -> str:
    return f"github:{repo}:issue#{issue_number}"


def make_evidence_id(artifact_id: str) -> str:
    # stable id for evidence derived from this event artifact
    return sha256_hex(f"ev|{artifact_id}")


def make_claim_id(claim_type: str, subject_id: str, predicate: str, obj_value: str, event_time: str) -> str:
    return sha256_hex(f"cl|{claim_type}|{subject_id}|{predicate}|{obj_value}|{event_time}")


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def build_event_quote(event_name: str, meta: Dict[str, Any]) -> str:
    """
    Turn GitHub event metadata into a human-readable grounding string.
    """
    parts = [f"event={event_name}"]

    if event_name == "assigned":
        assignee = meta.get("assignee")
        if assignee:
            parts.append(f"assignee={assignee}")

    if event_name in ("labeled", "unlabeled"):
        label = meta.get("label")
        if label:
            parts.append(f"label={label}")

    if event_name == "closed":
        parts.append("status=closed")
    if event_name == "reopened":
        parts.append("status=open")

    return " ".join(parts)


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise RuntimeError(f"Missing {INPUT_PATH}. Run dedup first.")

    # fresh outputs each run
    for p in (CLAIMS_OUT, EVIDENCE_OUT, REPORT_OUT):
        if os.path.exists(p):
            os.remove(p)

    total_artifacts = 0
    total_events = 0
    total_claims = 0
    total_evidence = 0

    extractor = {"name": "github_event_extractor", "version": "v1"}

    with open(INPUT_PATH, "r", encoding="utf-8") as fin:
        for line in fin:
            total_artifacts += 1
            art = json.loads(line)

            if art.get("type") != "event":
                continue

            total_events += 1

            repo = art.get("repo") or ""
            issue_number = int(art.get("issue_number") or -1)
            if issue_number == -1 or not repo:
                continue

            meta = art.get("metadata") or {}
            event_name = (meta.get("event") or "").strip()
            event_time = (art.get("created_at") or "").strip()

            if not event_name or not event_time:
                # Skip malformed events
                continue

            issue_id = make_issue_entity_id(repo, issue_number)

            # Evidence object (now with readable quote)
            ev_id = make_evidence_id(art["artifact_id"])
            evidence = {
                "evidence_id": ev_id,
                "artifact_id": art["artifact_id"],
                "timestamp": event_time,
                "url": art.get("url") or f"https://github.com/{repo}/issues/{issue_number}",
                "quote": build_event_quote(event_name, meta),  # ✅ key fix
                "start_char": 0,
                "end_char": 0,
            }

            claims_to_write: List[Dict[str, Any]] = []
            evidence_needed = False

            # STATUS claims
            if event_name == "closed":
                obj = "closed"
                claims_to_write.append({
                    "claim_id": make_claim_id("STATUS", issue_id, "status", obj, event_time),
                    "claim_type": "STATUS",
                    "subject": {"type": "Issue", "id": issue_id},
                    "predicate": "status",
                    "object": {"type": "Status", "value": obj},
                    "event_time": event_time,
                    "confidence": 0.99,
                    "evidence_ids": [ev_id],
                    "extractor": extractor,
                })
                evidence_needed = True

            elif event_name == "reopened":
                obj = "open"
                claims_to_write.append({
                    "claim_id": make_claim_id("STATUS", issue_id, "status", obj, event_time),
                    "claim_type": "STATUS",
                    "subject": {"type": "Issue", "id": issue_id},
                    "predicate": "status",
                    "object": {"type": "Status", "value": obj},
                    "event_time": event_time,
                    "confidence": 0.99,
                    "evidence_ids": [ev_id],
                    "extractor": extractor,
                })
                evidence_needed = True

            # ASSIGNED claims
            if event_name == "assigned":
                assignee = meta.get("assignee")
                if assignee:
                    claims_to_write.append({
                        "claim_id": make_claim_id("ASSIGNED", issue_id, "assigned_to", assignee, event_time),
                        "claim_type": "ASSIGNED",
                        "subject": {"type": "Issue", "id": issue_id},
                        "predicate": "assigned_to",
                        "object": {"type": "Person", "value": assignee},
                        "event_time": event_time,
                        "confidence": 0.98,
                        "evidence_ids": [ev_id],
                        "extractor": extractor,
                    })
                    evidence_needed = True

            # LABEL changes
            if event_name in ("labeled", "unlabeled"):
                label = meta.get("label")
                if label:
                    pred = "has_label" if event_name == "labeled" else "removed_label"
                    claims_to_write.append({
                        "claim_id": make_claim_id("LABEL", issue_id, pred, label, event_time),
                        "claim_type": "LABEL",
                        "subject": {"type": "Issue", "id": issue_id},
                        "predicate": pred,
                        "object": {"type": "Label", "value": label},
                        "event_time": event_time,
                        "confidence": 0.97,
                        "evidence_ids": [ev_id],
                        "extractor": extractor,
                    })
                    evidence_needed = True

            if claims_to_write:
                write_jsonl(CLAIMS_OUT, claims_to_write)
                total_claims += len(claims_to_write)

            if evidence_needed:
                write_jsonl(EVIDENCE_OUT, [evidence])
                total_evidence += 1

    report = {
        "input_artifacts": total_artifacts,
        "event_artifacts": total_events,
        "claims_written": total_claims,
        "evidence_written": total_evidence,
        "outputs": {
            "claims": CLAIMS_OUT,
            "evidence": EVIDENCE_OUT,
        }
    }

    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("Event extraction complete")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
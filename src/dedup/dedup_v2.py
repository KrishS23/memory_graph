"""
Stage: Artifact Deduplication v2 (reversible)
Purpose:
- Exact dedup by normalized clean_text
- Thread-local (issue_number) scope (prevents weird cross-thread merges)
- Keeps duplicates but marks them with duplicate_of + score
Outputs:
- artifacts_deduped.jsonl
- duplicate_edges.jsonl
- dedup_report.json
"""

import json
import os
import re
import hashlib
from typing import Dict, Any, Tuple, Optional


INPUT_PATH = "data/raw/artifacts.jsonl"
OUT_ARTIFACTS = "data/processed/artifacts_deduped.jsonl"
OUT_EDGES = "data/processed/duplicate_edges.jsonl"
OUT_REPORT = "data/processed/dedup_report.json"


# ---- Normalization helpers ----

WS_RE = re.compile(r"\s+")

def normalize_for_dedup(text: str) -> str:
    """
    Conservative normalization:
    - lowercase
    - collapse whitespace
    """
    t = (text or "").strip().lower()
    t = WS_RE.sub(" ", t)
    return t

def hash_text(t: str) -> str:
    return hashlib.sha256(t.encode("utf-8")).hexdigest()


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise RuntimeError(f"Missing input: {INPUT_PATH}. Run ingestion first.")

    os.makedirs(os.path.dirname(OUT_ARTIFACTS), exist_ok=True)

    # Key idea: dedup *per issue thread*
    # seen[(issue_number, text_hash)] = canonical_artifact_id
    seen: Dict[Tuple[int, str], str] = {}

    total = 0
    canonical = 0
    duplicates = 0

    # Store a few examples for the report
    examples = []

    with open(INPUT_PATH, "r", encoding="utf-8") as fin, \
         open(OUT_ARTIFACTS, "w", encoding="utf-8") as fout, \
         open(OUT_EDGES, "w", encoding="utf-8") as fdup:

        for line in fin:
            total += 1
            obj: Dict[str, Any] = json.loads(line)

            issue_num = int(obj.get("issue_number") or -1)
            clean = (obj.get("clean_text") or "").strip()

            # Default: not a duplicate
            obj["dedup"] = {"is_duplicate": False, "duplicate_of": None, "method": None, "score": None}

            # Only dedup items that actually have text (issue/comment).
            # Events typically have empty text and should pass through.
            if issue_num != -1 and clean:
                norm = normalize_for_dedup(clean)
                h = hash_text(norm)
                key = (issue_num, h)

                if key in seen:
                    # Mark as duplicate, keep it, and log an edge
                    duplicates += 1
                    canon_id = seen[key]
                    obj["dedup"] = {"is_duplicate": True, "duplicate_of": canon_id, "method": "exact_norm_hash", "score": 1.0}

                    edge = {
                        "type": "DuplicateOf",
                        "artifact_id": obj["artifact_id"],
                        "duplicate_of": canon_id,
                        "issue_number": issue_num,
                        "method": "exact_norm_hash",
                        "score": 1.0,
                    }
                    fdup.write(json.dumps(edge, ensure_ascii=False) + "\n")

                    if len(examples) < 5:
                        examples.append({
                            "issue_number": issue_num,
                            "duplicate_artifact_id": obj["artifact_id"],
                            "canonical_artifact_id": canon_id,
                            "text_preview": clean[:160],
                        })
                else:
                    # First time we see this text in this issue → canonical
                    canonical += 1
                    seen[key] = obj["artifact_id"]

            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    report = {
        "input_total": total,
        "canonical_count": canonical,
        "duplicate_count": duplicates,
        "duplicate_rate": (duplicates / total) if total else 0.0,
        "notes": [
            "Dedup is per issue thread (issue_number).",
            "This is reversible: duplicates are kept but marked + logged as DuplicateOf edges."
        ],
        "examples": examples,
        "outputs": {
            "artifacts": OUT_ARTIFACTS,
            "edges": OUT_EDGES,
        }
    }

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n✅ Dedup v2 complete")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
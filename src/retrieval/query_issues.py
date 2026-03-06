import json
import os
from typing import Dict, Any

CLAIMS_PATH = "data/processed/claims.jsonl"
OUT_PATH = "data/processed/current_state.jsonl"


def newer(a: str, b: str) -> bool:
    # ISO timestamps compare lexicographically
    return a >= b


def main() -> None:
    if not os.path.exists(CLAIMS_PATH):
        raise RuntimeError("Missing claims.jsonl — run extraction first.")

    state: Dict[str, Dict[str, Any]] = {}

    with open(CLAIMS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            claim = json.loads(line)

            subject = claim["subject"]["id"]
            pred = claim["predicate"]
            obj_val = claim["object"]["value"]
            ts = claim.get("event_time") or ""

            bucket = state.setdefault(subject, {
                "entity_id": subject,

                # track each field independently
                "status": {"value": None, "as_of": ""},
                "assigned_to": {"value": None, "as_of": ""},

                # labels computed cumulatively
                "labels": set(),
                "labels_as_of": "",

                # overall freshness (for display only)
                "last_updated": "",
            })

            # update overall last_updated
            if ts and newer(ts, bucket["last_updated"]):
                bucket["last_updated"] = ts

            # ---- STATUS ----
            if pred == "status":
                if ts and newer(ts, bucket["status"]["as_of"]):
                    bucket["status"] = {"value": obj_val, "as_of": ts}

            # ---- ASSIGNED ----
            elif pred == "assigned_to":
                if ts and newer(ts, bucket["assigned_to"]["as_of"]):
                    bucket["assigned_to"] = {"value": obj_val, "as_of": ts}

            # ---- LABELS ----
            elif pred == "has_label":
                bucket["labels"].add(obj_val)
                if ts and newer(ts, bucket["labels_as_of"]):
                    bucket["labels_as_of"] = ts

            elif pred == "removed_label":
                bucket["labels"].discard(obj_val)
                if ts and newer(ts, bucket["labels_as_of"]):
                    bucket["labels_as_of"] = ts

    # write output
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for ent in state.values():
            ent["labels"] = sorted(list(ent["labels"]))
            f.write(json.dumps(ent, ensure_ascii=False) + "\n")

    print(f"✅ Current state built for {len(state)} issues")
    print(f"Output: {OUT_PATH}")


if __name__ == "__main__":
    main()
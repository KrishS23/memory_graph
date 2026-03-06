import json
import os
from typing import Dict, Any

CLAIMS_PATH = "data/processed/claims.jsonl"
OUT_PATH = "data/processed/current_state.jsonl"


def main() -> None:
    if not os.path.exists(CLAIMS_PATH):
        raise RuntimeError("Missing claims.jsonl — run extraction first.")

    # entity_id -> latest fields
    state: Dict[str, Dict[str, Any]] = {}

    with open(CLAIMS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            claim = json.loads(line)

            subject = claim["subject"]["id"]
            pred = claim["predicate"]
            obj_val = claim["object"]["value"]
            ts = claim["event_time"] or ""

            bucket = state.setdefault(subject, {
                "entity_id": subject,
                "current_status": None,
                "assigned_to": None,
                "labels": set(),
                "last_updated": "",
            })

            # Only update if newer timestamp
            if ts >= bucket["last_updated"]:
                if pred == "status":
                    bucket["current_status"] = obj_val
                elif pred == "assigned_to":
                    bucket["assigned_to"] = obj_val
                elif pred == "has_label":
                    bucket["labels"].add(obj_val)
                elif pred == "removed_label":
                    bucket["labels"].discard(obj_val)

                bucket["last_updated"] = ts

    # write output
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for ent in state.values():
            ent["labels"] = sorted(list(ent["labels"]))
            f.write(json.dumps(ent, ensure_ascii=False) + "\n")

    print(f"Current state built for {len(state)} issues")
    print(f"Output: {OUT_PATH}")


if __name__ == "__main__":
    main()
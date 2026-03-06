"""
Stage: Artifact Deduplication
Purpose: Remove exact-duplicate clean_text artifacts.
"""

import json
import os
from typing import Dict, Set


INPUT_PATH = "data/raw/artifacts.jsonl"
OUTPUT_PATH = "data/processed/artifacts_deduped.jsonl"
REPORT_PATH = "data/processed/dedup_report.json"


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise RuntimeError(f"Missing input file: {INPUT_PATH}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    seen_texts: Set[str] = set()

    kept = 0
    dropped = 0
    total = 0

    # overwrite output each run
    with open(INPUT_PATH, "r", encoding="utf-8") as fin, \
         open(OUTPUT_PATH, "w", encoding="utf-8") as fout:

        for line in fin:
            total += 1
            obj = json.loads(line)

            text = (obj.get("clean_text") or "").strip()

            # Only dedup text-bearing artifacts
            if text:
                if text in seen_texts:
                    dropped += 1
                    continue
                seen_texts.add(text)

            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            kept += 1

    report = {
        "input_total": total,
        "kept": kept,
        "dropped_duplicates": dropped,
        "unique_texts": len(seen_texts),
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n✅ Dedup complete")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
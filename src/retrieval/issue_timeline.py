import json
import sys

CLAIMS_PATH = "data/processed/claims.jsonl"
EVIDENCE_PATH = "data/processed/evidence.jsonl"


def load_claims():
    claims = []
    with open(CLAIMS_PATH, "r") as f:
        for line in f:
            claims.append(json.loads(line))
    return claims


def load_evidence():
    evidence_map = {}
    with open(EVIDENCE_PATH, "r") as f:
        for line in f:
            e = json.loads(line)
            evidence_map[e["evidence_id"]] = e
    return evidence_map


def main():
    if len(sys.argv) < 2:
        print("Usage: python issue_timeline.py <issue_number>")
        return

    issue_number = sys.argv[1]
    entity_id = f"github:rust-lang/rust:issue#{issue_number}"

    claims = load_claims()
    evidence = load_evidence()

    issue_events = []

    for c in claims:
        if c["subject"]["id"] != entity_id:
            continue

        ev = evidence.get(c["evidence_ids"][0])

        issue_events.append({
            "time": c["event_time"],
            "predicate": c["predicate"],
            "value": c["object"]["value"],
            "evidence": ev["quote"] if ev else ""
        })

    # sort by time newest → oldest
    issue_events.sort(key=lambda x: x["time"], reverse=True)

    print(f"\nTimeline for issue {issue_number}\n")

    for e in issue_events:
        print(f"{e['time']}   {e['predicate']:<15} {e['value']}")

    print()


if __name__ == "__main__":
    main()
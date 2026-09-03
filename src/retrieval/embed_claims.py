"""
Stage: Claim Embedding
Purpose: Generate sentence embeddings for every claim, combining the
         structured triple with its grounding evidence quote.
"""

import json
import os
from typing import Any, Dict, List

import numpy as np
from sentence_transformers import SentenceTransformer

CLAIMS_PATH = "data/processed/claims.jsonl"
EVIDENCE_PATH = "data/processed/evidence.jsonl"
EMBEDDINGS_OUT = "data/processed/claim_embeddings.npy"
ID_MAP_OUT = "data/processed/claim_id_map.json"
MODEL_NAME = "all-MiniLM-L6-v2"


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def build_claim_text(claim: Dict[str, Any], evidence_by_id: Dict[str, Dict[str, Any]]) -> str:
    """
    Turn a structured claim + its evidence into a single text string
    suitable for embedding.
    """
    subject = claim["subject"]["id"]
    predicate = claim["predicate"]
    obj = claim["object"]["value"]

    quote = ""
    ev_ids = claim.get("evidence_ids") or []
    if ev_ids and ev_ids[0] in evidence_by_id:
        quote = evidence_by_id[ev_ids[0]].get("quote", "")

    return f"{subject} {predicate} {obj}. {quote}".strip()


def main() -> None:
    if not os.path.exists(CLAIMS_PATH):
        raise RuntimeError(f"Missing {CLAIMS_PATH}. Run extraction first.")

    claims = load_jsonl(CLAIMS_PATH)
    evidence = load_jsonl(EVIDENCE_PATH)
    evidence_by_id = {e["evidence_id"]: e for e in evidence}

    texts = [build_claim_text(c, evidence_by_id) for c in claims]
    claim_ids = [c["claim_id"] for c in claims]

    print(f"Embedding {len(texts)} claims with {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True)

    os.makedirs(os.path.dirname(EMBEDDINGS_OUT), exist_ok=True)
    np.save(EMBEDDINGS_OUT, embeddings)

    with open(ID_MAP_OUT, "w", encoding="utf-8") as f:
        json.dump(claim_ids, f, indent=2)  # row index -> claim_id, by position

    print(f"Saved {embeddings.shape[0]} embeddings (dim={embeddings.shape[1]})")
    print(f"Embeddings: {EMBEDDINGS_OUT}")
    print(f"ID map: {ID_MAP_OUT}")


if __name__ == "__main__":
    main()
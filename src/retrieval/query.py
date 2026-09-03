"""
Stage: Retrieval
Purpose: Given a natural-language query, retrieve the top-k most
         relevant claims and their grounding evidence.
"""

import json
from typing import Any, Dict, List

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

CLAIMS_PATH = "data/processed/claims.jsonl"
EVIDENCE_PATH = "data/processed/evidence.jsonl"
INDEX_PATH = "data/processed/faiss.index"
ID_MAP_PATH = "data/processed/claim_id_map.json"
MODEL_NAME = "all-MiniLM-L6-v2"


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def retrieve(query: str, k: int = 5) -> List[Dict[str, Any]]:
    model = SentenceTransformer(MODEL_NAME)
    index = faiss.read_index(INDEX_PATH)

    with open(ID_MAP_PATH, "r", encoding="utf-8") as f:
        claim_ids = json.load(f)

    claims_by_id = {c["claim_id"]: c for c in load_jsonl(CLAIMS_PATH)}
    evidence_by_id = {e["evidence_id"]: e for e in load_jsonl(EVIDENCE_PATH)}

    query_vec = model.encode([query], convert_to_numpy=True).astype("float32")
    faiss.normalize_L2(query_vec)

    scores, indices = index.search(query_vec, k)

    results = []
    for score, row in zip(scores[0], indices[0]):
        if row == -1:
            continue
        claim = claims_by_id[claim_ids[row]]
        ev_id = (claim.get("evidence_ids") or [None])[0]
        results.append({
            "score": float(score),
            "claim": claim,
            "evidence": evidence_by_id.get(ev_id),
        })
    return results


if __name__ == "__main__":
    q = input("Query: ")
    for r in retrieve(q):
        print(f"\n[{r['score']:.3f}] {r['claim']['subject']['id']} "
              f"{r['claim']['predicate']} {r['claim']['object']['value']}")
        if r["evidence"]:
            print(f"   evidence: {r['evidence']['quote']} ({r['evidence']['url']})")
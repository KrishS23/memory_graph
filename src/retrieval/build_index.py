"""
Stage: Index Building
Purpose: Build a FAISS index over claim embeddings for fast
         cosine-similarity retrieval.
"""

import os

import faiss
import numpy as np

EMBEDDINGS_PATH = "data/processed/claim_embeddings.npy"
INDEX_OUT = "data/processed/faiss.index"


def main() -> None:
    if not os.path.exists(EMBEDDINGS_PATH):
        raise RuntimeError(f"Missing {EMBEDDINGS_PATH}. Run embed_claims.py first.")

    embeddings = np.load(EMBEDDINGS_PATH).astype("float32")

    # Normalize to unit length so inner product == cosine similarity
    faiss.normalize_L2(embeddings)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    os.makedirs(os.path.dirname(INDEX_OUT), exist_ok=True)
    faiss.write_index(index, INDEX_OUT)

    print(f"✅ Indexed {index.ntotal} claim vectors (dim={dim})")
    print(f"Saved to {INDEX_OUT}")


if __name__ == "__main__":
    main()
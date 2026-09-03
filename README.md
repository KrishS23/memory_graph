# Memory Graph from GitHub Issues

A pipeline that converts GitHub issue activity into a structured, evidence-grounded memory graph — with semantic search and LLM-based grounded Q&A on top.

Every claim in the graph is traceable back to the GitHub artifact that produced it.

## Pipeline

```
GitHub Issues → Ingest → Dedup → Extract Claims → Resolve State
              → Embed → Index → Retrieve → Generate
              → Visualize
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file:

```
GITHUB_TOKEN=your_github_token
GEMINI_API_KEY=your_gemini_api_key
```

## Usage

Run each stage in order:

```bash
python src/ingest/select_issues.py        # pull issues from GitHub
python src/dedup/dedup_artifacts.py       # deduplicate artifacts
python src/extract/extract_events.py      # extract claims + evidence
python src/resolve/build_current_state.py # resolve current state

python src/retrieval/embed_claims.py      # embed claims
python src/retrieval/build_index.py       # build FAISS index
python src/retrieval/query.py             # semantic search over claims
python src/retrieval/generate.py          # grounded Q&A via Gemini

streamlit run src/visualisation/app.py    # interactive explorer
```

## How it works

- **Extraction** — GitHub issue events (labels, assignees, status changes) are turned into structured claims: `subject → predicate → object`, each linked to its source evidence (quote + URL).
- **Deduplication** — artifacts are normalized and deduplicated before extraction.
- **Retrieval** — claims are embedded (`all-MiniLM-L6-v2`) and indexed with FAISS for semantic search.
- **Generation** — Gemini answers natural-language questions using *only* retrieved claims as context, so answers stay grounded in real evidence.
- **Visualization** — a Streamlit app for exploring the graph and searching it.

## Repository Structure

```
src/
├── ingest/        # pull raw issue data from GitHub
├── dedup/         # deduplicate artifacts
├── extract/       # extract structured claims + evidence
├── resolve/       # compute current state from claims
├── retrieval/     # embed, index, retrieve, generate
└── visualisation/ # Streamlit explorer

data/
├── raw/           # ingested artifacts
└── processed/     # claims, evidence, embeddings, index
```

## Example

```
Issue#153101 --has_label--> E-help-wanted
Issue#153101 --assigned_to--> JayanAXHF
Issue#153101 --status--> closed
```
"""
Stage: Grounded Generation
Purpose: Answer a natural-language question using ONLY retrieved,
         evidence-linked claims — never facts outside what was retrieved.
"""

import os
from typing import Any, Dict, List

from google import genai

from src.retrieval.query import retrieve
from dotenv import load_dotenv

load_dotenv()

MODEL_NAME = "gemini-3-flash"


def build_evidence_block(results: List[Dict[str, Any]]) -> str:
    lines = []
    for i, r in enumerate(results, start=1):
        claim = r["claim"]
        ev = r["evidence"]
        line = (
            f"{i}. {claim['subject']['id']} {claim['predicate']} "
            f"{claim['object']['value']}"
        )
        if ev:
            line += f" (quote: \"{ev['quote']}\", source: {ev['url']})"
        lines.append(line)
    return "\n".join(lines)


def build_prompt(query: str, results: List[Dict[str, Any]]) -> str:
    evidence_block = build_evidence_block(results)
    return f"""You are answering questions about GitHub issue activity using ONLY the evidence below.
If the evidence doesn't contain the answer, say "I don't have enough information."
Never state anything not directly supported by the evidence. Cite evidence numbers in your answer.

Evidence:
{evidence_block}

Question: {query}

Answer:"""


def generate_answer(query: str, k: int = 5) -> str:
    results = retrieve(query, k=k)
    if not results:
        return "No relevant claims were found for this query."

    prompt = build_prompt(query, results)

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
    )
    return response.text


if __name__ == "__main__":
    q = input("Query: ")
    print("\n" + generate_answer(q))
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from app.rag.retrieval import MIN_RELEVANCE_SCORE, retrieve_context


TOP_K = 5
QUESTIONS_PATH = (
    Path(__file__).resolve().parents[2]
    / "evaluation"
    / "rag_questions.json"
)


async def main() -> None:
    questions = json.loads(
        QUESTIONS_PATH.read_text(encoding="utf-8")
    )

    hits = 0
    reciprocal_rank_sum = 0.0
    results = []

    for question in questions:
        contexts = await retrieve_context(
            question["question"],
            limit=TOP_K,
        )

        found_rank = next(
            (
                rank
                for rank, item in enumerate(contexts, start=1)
                if item["source"] == question["expected_source"]
                and item["heading"] == question["expected_heading"]
            ),
            None,
        )

        hit = found_rank is not None
        if hit:
            hits += 1
            reciprocal_rank_sum += 1.0 / found_rank

        results.append(
            {
                "id": question["id"],
                "expected_source": question["expected_source"],
                "expected_heading": question["expected_heading"],
                "hit": hit,
                "found_rank": found_rank,
                "retrieved_sources": [
                    {
                        "source": item["source"],
                        "heading": item["heading"],
                        "score": item["score"],
                    }
                    for item in contexts
                ],
            }
        )

    total = len(questions)
    hit_rate = hits / total if total else 0
    mrr = reciprocal_rank_sum / total if total else 0

    print()
    print("=" * 70)
    print("RETAILPULSE RAG RETRIEVAL EVALUATION")
    print("=" * 70)
    print(f"Questions : {total}")
    print(f"HitRate@{TOP_K}: {hit_rate:.4f}")
    print(f"MRR       : {mrr:.4f}")
    print(f"Minimum relevance score: {MIN_RELEVANCE_SCORE}")
    print()

    for result in results:
        print(
            result["id"],
            "|",
            "PASS" if result["hit"] else "FAIL",
            "| rank:",
            result["found_rank"],
        )
        if not result["hit"]:
            print(
                f"  Expected: {result['expected_source']}"
                f" | {result['expected_heading']}"
            )
            if not result["retrieved_sources"]:
                print("  No results survived the relevance filter.")
            for rank, item in enumerate(result["retrieved_sources"], start=1):
                print(
                    f"  {rank}. source={item['source']!r}"
                    f" | heading={item['heading']!r}"
                    f" | score={item['score']}"
                )
            if any(
                item["heading"] is None
                for item in result["retrieved_sources"]
            ):
                print("  Missing heading metadata: re-ingest the development collection.")

    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())

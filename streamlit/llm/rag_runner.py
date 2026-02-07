# llm/rag_runner.py
from __future__ import annotations

import uuid
from typing import Any, Iterable

import pandas as pd
from qdrant_client import QdrantClient

from .embeddings import OpenAIEmbedder
from .semantic_index import index_rows, semantic_search
from .summarizer import OpenAISummarizer


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _df_to_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.to_dict(orient="records")


def _rows_to_documents(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    EXACT order-preserving mapping.
    Order MUST match what the LLM saw.
    """
    docs: list[dict[str, Any]] = []
    for r in rows:
        docs.append(
            {
                "external_id": r.get("external_id"),
                "title": r.get("title"),
                "published_at": r.get("published_at"),
                "url": r.get("url"),
                "text": r.get("fulltext") or r.get("text") or "",
                # Debug / Trace
                "_rank": r.get("_rank"),
                "_score": r.get("_score"),
            }
        )
    return docs


# -------------------------------------------------
# Main RAG
# -------------------------------------------------
def run_rag_on_dataframe(
    df: pd.DataFrame,
    *,
    schema: dict,
    prompts: dict,
    query: str,
    topic: str,
    openai_key: str,
    qdrant_url: str,
    qdrant_api_key: str,
    prompt_key: str = "news_summary",
    embed_dim: int = 1536,
    top_k: int = 5,
    max_docs_to_index: int = 2000,
    cleanup_collection: bool = True,
) -> dict[str, Any]:

    if df.empty:
        return {"summary": "", "claims": [], "raw": "", "docs": [], "hits": []}

    df_small = df.head(max_docs_to_index).copy()
    rows = _df_to_rows(df_small)

    # require external_id
    rows = [r for r in rows if r.get("external_id")]
    if not rows:
        return {"summary": "", "claims": [], "raw": "", "docs": [], "hits": []}

    embedder = OpenAIEmbedder(openai_key)
    summarizer = OpenAISummarizer(
        api_key=openai_key,
        prompts=prompts,
        prompt_key=prompt_key,
        topic=topic,
    )

    qdrant = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
    collection = "ui_tmp_" + uuid.uuid4().hex[:10]

    # -------------------------------------------------
    # 1) Index
    # -------------------------------------------------
    index_rows(
        rows=rows,
        schema=schema,
        collection_name=collection,
        embedder=embedder,
        qdrant=qdrant,
        embed_dim=embed_dim,
    )

    # -------------------------------------------------
    # 2) Semantic search (SORTED!)
    # -------------------------------------------------
    qvec = embedder.embed_query(query)
    hits = semantic_search(qdrant, collection, qvec, top_k)

    # 🔒 HARD GUARANTEE: score order
    hits = sorted(hits, key=lambda h: float(h.score), reverse=True)

    # -------------------------------------------------
    # 3) Build EXACT doc list for LLM + UI
    # -------------------------------------------------
    by_external_id = {r["external_id"]: r for r in rows}

    hit_rows: list[dict[str, Any]] = []
    for rank, h in enumerate(hits, start=1):
        payload = h.payload or {}
        ext_id = payload.get("external_id")

        if ext_id and ext_id in by_external_id:
            base = by_external_id[ext_id].copy()
            base["_rank"] = rank
            base["_score"] = float(h.score)
            hit_rows.append(base)

    # These rows are the SINGLE SOURCE OF TRUTH
    docs = _rows_to_documents(hit_rows)

    # -------------------------------------------------
    # 4) Summarize (LLM sees EXACT same docs)
    # -------------------------------------------------
    pack = summarizer.summarize(query, docs)

    # -------------------------------------------------
    # 5) Cleanup
    # -------------------------------------------------
    if cleanup_collection:
        try:
            qdrant.delete_collection(collection)
        except Exception:
            pass

    return {
        "summary": pack.get("summary", ""),
        "claims": pack.get("claims", []),
        "raw": pack.get("raw", ""),
        "docs": docs,          # ← SAME objects, SAME order
        "hits": [
            {
                "rank": d["_rank"],
                "score": d["_score"],
                "external_id": d["external_id"],
            }
            for d in docs
        ],
        "meta": {
            "collection": collection,
            "top_k": top_k,
            "indexed": len(rows),
            "used_for_llm": len(docs),
        },
    }


def run_rag_sync(*args, **kwargs):
    return run_rag_on_dataframe(*args, **kwargs)

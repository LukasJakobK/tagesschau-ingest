# rag/daily_ingest.py

import os
import uuid
import json
from datetime import datetime
from pathlib import Path

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import libsql_client


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
COLLECTION_NAME = "articles_new"
BATCH_SIZE = 200
MAX_CHARS = 2000
EMBED_MODEL_NAME = "paraphrase-multilingual-mpnet-base-v2"

SCHEMA_PATH = Path("config/article_schema.json")


# ------------------------------------------------------------
# LOAD SCHEMA
# ------------------------------------------------------------
with SCHEMA_PATH.open("r", encoding="utf-8") as f:
    ARTICLE_SCHEMA = json.load(f)


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def stable_point_id(external_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, external_id))


def build_payload(row: dict, schema: dict) -> dict:
    payload = {}
    for col, cfg in schema["columns"].items():
        if cfg["role"] == "payload":
            val = row.get(col)
            if val is not None:
                payload[col] = val
    return payload


def build_embedding_text(row: dict, schema: dict) -> str:
    parts = []
    for col, cfg in schema["columns"].items():
        if cfg["role"] == "embedding":
            val = row.get(col)
            if val:
                parts.append(str(val))
    return "\n\n".join(parts)


# ------------------------------------------------------------
# MAIN (SYNC, CI-SAFE)
# ------------------------------------------------------------
def main() -> None:
    # ---- ENV
    TURSO_DB_URL = os.environ["TURSO_DB_URL"].replace("libsql://", "https://")
    TURSO_AUTH_TOKEN = os.environ["TURSO_AUTH_TOKEN"]
    QDRANT_URL = os.environ["QADRANT_ENDPOINT"]
    QDRANT_API_KEY = os.environ["QADRANT_API_KEY"]

    # ---- Clients (SYNC ONLY)
    db = libsql_client.create_client(
        url=TURSO_DB_URL,
        auth_token=TURSO_AUTH_TOKEN,
    )

    qdrant = QdrantClient(
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY,
        check_compatibility=False,  # CI-stabil
    )

    embedder = SentenceTransformer(EMBED_MODEL_NAME)

    # ---- Query
    SELECT_SQL = f"""
        SELECT *
        FROM articles
        WHERE
            fulltext IS NOT NULL
            AND embedded_at IS NULL
        ORDER BY published_at ASC
        LIMIT {BATCH_SIZE}
    """

    rs = db.execute(SELECT_SQL)

    if not rs.rows:
        print("🟢 No new articles to embed.")
        return

    rows = [dict(zip(rs.columns, r)) for r in rs.rows]

    texts = [
        build_embedding_text(row, ARTICLE_SCHEMA)[:MAX_CHARS]
        for row in rows
    ]

    embeddings = embedder.encode(
        texts,
        batch_size=64,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    points = []
    external_ids = []

    for i, row in enumerate(rows):
        external_id = row["external_id"]
        external_ids.append(external_id)

        payload = build_payload(row, ARTICLE_SCHEMA)
        payload.update(
            {
                "embedded_at": datetime.utcnow().isoformat(),
                "embedding_model": EMBED_MODEL_NAME,
            }
        )

        points.append(
            PointStruct(
                id=stable_point_id(external_id),
                vector=embeddings[i].tolist(),
                payload=payload,
            )
        )

    # ---- Qdrant upsert
    qdrant.upsert(
        collection_name=COLLECTION_NAME,
        points=points,
    )

    # ---- Mark embedded
    placeholders = ",".join(["?"] * len(external_ids))
    db.execute(
        f"""
        UPDATE articles
        SET embedded_at = CURRENT_TIMESTAMP
        WHERE external_id IN ({placeholders})
        """,
        external_ids,
    )

    db.close()

    print(f"✅ Embedded {len(external_ids)} new articles.")


# ------------------------------------------------------------
# BOOTSTRAP
# ------------------------------------------------------------
if __name__ == "__main__":
    main()

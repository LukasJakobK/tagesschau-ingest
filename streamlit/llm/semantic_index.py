# rag/semantic_index.py

import uuid
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

from .schema import build_embedding_text, build_payload


def index_rows(
    rows: list[dict],
    schema: dict,
    collection_name: str,
    embedder,
    qdrant: QdrantClient,
    embed_dim: int,
):
    if qdrant.collection_exists(collection_name):
        qdrant.delete_collection(collection_name)

    qdrant.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=embed_dim, distance=Distance.COSINE),
    )

    texts, payloads, ids = [], [], []

    for row in rows:
        text = build_embedding_text(row, schema)
        if not text.strip():
            continue

        payload = build_payload(row, schema)
        if "external_id" not in payload:
            continue

        texts.append(text)
        payloads.append(payload)

        pid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(payload["external_id"])))
        ids.append(pid)

    vectors = embedder.embed_documents(texts)

    points = [
        PointStruct(id=ids[i], vector=vectors[i], payload=payloads[i])
        for i in range(len(ids))
    ]

    qdrant.upsert(collection_name=collection_name, points=points)


def semantic_search(qdrant, collection: str, query_vector: list[float], top_k: int):
    res = qdrant.query_points(
        collection_name=collection,
        query=query_vector,
        limit=top_k,
    )
    return res.points

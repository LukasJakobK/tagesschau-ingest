from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, Range, MatchAny


class QdrantSearchClient:
    def __init__(self, url: str, api_key: str, collection: str):
        self.client = QdrantClient(url=url, api_key=api_key)
        self.collection = collection

    def search(
        self,
        vector: list[float],
        top_k: int,
        qdrant_filter: Filter | None,
    ) -> list[dict]:
        results = self.client.search(
            collection_name=self.collection,
            query_vector=vector,
            limit=top_k,
            with_payload=True,
            score_threshold=None,
            query_filter=qdrant_filter,
        )

        return [
            {
                "score": r.score,
                "payload": r.payload,
                "id": r.id,
            }
            for r in results
        ]

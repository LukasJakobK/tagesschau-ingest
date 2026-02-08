from typing import List
from datetime import datetime

from qdrant_client.models import (
    Filter,
    FieldCondition,
    MatchAny,
    DatetimeRange,
)

from Agents.Agent_1.config.schema import SearchPlan
from .qadrant_client import QdrantSearchClient


class SearchExecutor:
    def __init__(self, qdrant_client: QdrantSearchClient, embedder):
        self.qdrant = qdrant_client
        self.embedder = embedder

    def _build_filter(self, plan: SearchPlan) -> Filter | None:
        if not plan.filters:
            return None

        f = plan.filters
        conditions = []

        if f.published_at:
            gte = f.published_at.get("gte")
            lte = f.published_at.get("lte")
            conditions.append(
                FieldCondition(
                    key="published_at",
                    range=DatetimeRange(
                        gte=datetime.fromisoformat(gte) if gte else None,
                        lte=datetime.fromisoformat(lte) if lte else None,
                    ),
                )
            )

        for field in [
            "region_by_url",
            "subregion_by_url",
            "region_by_api",
            "region_by_source",
            "source",
            "ressort",
            "type",
        ]:
            values = getattr(f, field, None)
            if values:
                conditions.append(
                    FieldCondition(
                        key=field,
                        match=MatchAny(any=values),
                    )
                )

        return Filter(must=conditions) if conditions else None

    def execute(self, plan: SearchPlan) -> List[dict]:
        # 1) Query-Vektor
        vectors = []
        weights = []
        for q in plan.queries:
            v = self.embedder.encode(q.text, normalize_embeddings=True)
            vectors.append(v)
            weights.append(q.weight)

        query_vector = sum(w * v for w, v in zip(weights, vectors)) / sum(weights)

        # 2) Filter
        qdrant_filter = self._build_filter(plan)

        # 3) Query
        if qdrant_filter is None:
            # Wrapper kann "ohne Filter"
            res = self.qdrant.query_points(
                query=query_vector.tolist(),
                limit=plan.top_k,
            )
        else:
            # SDK-call MIT KORREKTEM PARAMETERNAME
            res = self.qdrant.client.query_points(
                collection_name=self.qdrant.collection,
                query=query_vector.tolist(),
                limit=plan.top_k,
                query_filter=qdrant_filter,   # ✅ NICHT filter=
            )

        points = res.points or []

        if plan.sort == "time_desc":
            points.sort(key=lambda p: p.payload.get("published_at", ""), reverse=True)

        return [{"score": p.score, "payload": p.payload} for p in points]
